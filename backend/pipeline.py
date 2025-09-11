import argparse
import json
import logging
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np

# Aggiunge la directory corrente al path per garantire che gli import locali funzionino
sys.path.insert(0, str(Path(__file__).parent))

from data_ingestion import DataIntegrator
from ml_forecast import FeatureEngineering, RiskPredictor
from post_processor import PredictionPostProcessor
from data_exporter import DataExporter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class MLPipeline:
    """Gestisce l'esecuzione sequenziale delle fasi del pipeline ML."""
    
    def __init__(self, config_path: str = "config.json"):
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.data_integrator = DataIntegrator(config_path)
        self.predictor = RiskPredictor(self.config.get('ml_params', {}))
        self.post_processor = PredictionPostProcessor(self.config)
        self.exporter = DataExporter('frontend/data')
        self.data = {}

    def run(self, force_training: bool = False):
        """Esegue il pipeline completo: dati -> training -> predizione -> export."""
        logger.info("Avvio pipeline Georisk Sentinel...")
        try:
            self._load_data()
            self._manage_model(force_training)
            self._generate_predictions()
            self._publish_results()
            logger.info("Pipeline completato con successo.")
        except Exception as e:
            logger.error(f"Esecuzione pipeline fallita: {e}", exc_info=True)
            raise

    def _load_data(self):
        """Carica e prepara i dati necessari per il training."""
        logger.info("Fase 1: Caricamento dati...")
        self.data['events'], self.data['aux'] = self.data_integrator.prepare_training_dataset()
        logger.info(f"Caricati {len(self.data['events'])} eventi per il training.")

    def _manage_model(self, force_training: bool):
        """Carica un modello pre-addestrato o ne avvia il training."""
        logger.info("Fase 2: Gestione modello...")
        model_path = Path(self.config['project_paths']['model_artifact'])
        
        if model_path.exists() and not force_training:
            logger.info(f"Caricamento modello da '{model_path}'...")
            self.predictor.load_model(str(model_path))
        else:
            logger.info("Nessun modello trovato o training forzato. Avvio addestramento...")
            feature_engineer = FeatureEngineering(
                self.config['ml_params'],
                dem_path=self.data['aux'].get('dem_path')
            )
            self.predictor.feature_engineer = feature_engineer
            
            X, y = self.predictor.prepare_training_data(self.data['events'])
            metrics = self.predictor.train(X, y)
            
            logger.info(f"Training completato. Metriche: R2={metrics['test_r2']:.3f}, RMSE={metrics['test_rmse']:.2f}")
            self.predictor.save_model(str(model_path))

    def _generate_predictions(self):
        """Genera le predizioni di rischio su una griglia geografica."""
        logger.info("Fase 3: Generazione predizioni...")
        cfg = self.config['ml_params']['prediction']
        bounds = cfg['lombardy_bounds']
        
        lats = np.arange(bounds['lat_min'], bounds['lat_max'], cfg['grid_resolution_deg'])
        lons = np.arange(bounds['lon_min'], bounds['lon_max'], cfg['grid_resolution_deg'])
        points = [(lat, lon) for lat in lats for lon in lons]
        
        predictions_df = self.predictor.predict(points)
        
        threshold = cfg['min_risk_score_threshold']
        predictions_df = predictions_df[predictions_df['risk_score'] >= threshold]
        
        if predictions_df.empty:
            logger.warning("Nessuna area ha superato la soglia di rischio. Non verranno generate allerte.")
            self.data['predictions'] = gpd.GeoDataFrame()
            return

        predictions_gdf = gpd.GeoDataFrame(
            predictions_df,
            geometry=gpd.points_from_xy(predictions_df.longitude, predictions_df.latitude),
            crs='EPSG:4326'
        )
        
        self.data['predictions'] = self.post_processor.enrich_predictions(predictions_gdf)
        logger.info(f"Generate {len(self.data['predictions'])} allerte valide.")

    def _publish_results(self):
        """Esporta i risultati finali in un formato consumabile dal frontend."""
        logger.info("Fase 4: Pubblicazione risultati...")
        if self.data.get('predictions', gpd.GeoDataFrame()).empty:
            logger.warning("Nessuna predizione da pubblicare.")
            # Esporta comunque un file vuoto per mantenere il frontend consistente
            self.exporter.export_geodataframe(gpd.GeoDataFrame(), "Georisk Sentinel Lombardia")
            return
        
        result = self.exporter.export_geodataframe(
            self.data['predictions'],
            "Georisk Sentinel Lombardia - Predizioni ML"
        )
        logger.info(f"Pubblicati {result['numero_allerte']} allerte.")


def main():
    """Entry point per l'esecuzione del pipeline da linea di comando."""
    parser = argparse.ArgumentParser(description='Georisk Sentinel ML Pipeline')
    parser.add_argument('--train', action='store_true', help='Forza il re-training del modello anche se ne esiste uno salvato.')
    args = parser.parse_args()
    
    pipeline = MLPipeline("config.json")
    pipeline.run(force_training=args.train)


if __name__ == "__main__":
    main()