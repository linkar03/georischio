"""
Georisk Sentinel Lombardia - ML Pipeline Orchestrator (Refactored Version)
Pipeline guidato da configurazione: download dati → training → predizione → pubblicazione.
"""
import sys
import json
import numpy as np
import geopandas as gpd
from typing import Dict
import argparse
from pathlib import Path
from datetime import datetime
import logging
from jinja2 import Environment, FileSystemLoader
sys.path.insert(0, str(Path(__file__).parent))
from dati import DataIntegrator
from ml_predittore import RiskPredictor, FeatureEngineering
from post_processor import PredictionPostProcessor
from arcgis_publisher import FreeAccountPublisher

# config
Path("logs").mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/ml_pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MLPipeline:
    "Orchestratore del pipeline Machine Learning completo, guidato da file di configurazione."
    
    def __init__(self, config_path: str = "config.json"):
        """Inizializza il pipeline con gestione errori."""
        self.config = self._load_config(config_path)
        if not self.config:
            raise ValueError("Impossibile caricare o interpretare il file di configurazione.")
        
        self._setup_directories()
        
        
        try:
            self.data_integrator = DataIntegrator(config_path) 
            
            self.predictor = RiskPredictor(self.config.get('ml_params', {}))
            self.post_processor = PredictionPostProcessor(self.config)
            
            frontend_path = self.config.get('project_paths', {}).get('frontend_data', 'frontend/data')
            self.publisher = FreeAccountPublisher(output_dir=frontend_path)
            
            templates_dir = Path(self.config.get('project_paths', {}).get('templates_dir', 'templates'))
            if templates_dir.exists():
                self.jinja_env = Environment(loader=FileSystemLoader(str(templates_dir)))
            else:
                self.jinja_env = None
                logger.warning(f"Directory dei template '{templates_dir}' non trovata. Report disabilitati.")
                
        except Exception as e:
            logger.exception(f"Errore durante l'inizializzazione dei componenti del pipeline: {e}")
            raise
        
        self.pipeline_state = {}

    def _load_config(self, config_path: str) -> dict:
        "Carica il file di configurazione JSON in modo sicuro."
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"File di configurazione '{config_path}' non trovato.")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Errore nel parsing del file JSON '{config_path}': {e}")
            return None

    def _setup_directories(self):
        "Crea tutte le directory necessarie definite nella configurazione."
        if 'project_paths' not in self.config:
            logger.warning("La sezione 'project_paths' non è presente nel config. Le directory non verranno create.")
            return
        
        paths_to_create = {k: v for k, v in self.config['project_paths'].items() if k not in ['model_artifact', 'templates_dir']}
        
        for name, path_str in paths_to_create.items():
            try:
                Path(path_str).mkdir(parents=True, exist_ok=True)
                logger.info(f"Directory '{name}' assicurata: {path_str}")
            except OSError as e:
                logger.error(f"Impossibile creare la directory '{path_str}': {e}")
    
    def step1_download_data(self):
        "Step 1: Download e preparazione dati di training."
        logger.info("="*25 + " DOWNLOAD DATI " + "="*25)
        self.pipeline_state['events_gdf'], self.pipeline_state['aux_data'] = self.data_integrator.prepare_training_dataset()
        logger.info(f"✅ Step 1 completato: {len(self.pipeline_state['events_gdf'])} eventi caricati.")

    def step2_train_model(self, force_retrain: bool = False):
        "Step 2: Carica un modello esistente o ne addestra uno nuovo."
        logger.info("="*25 + " TRAINING MODELLO " + "="*25)
        model_path = Path(self.config['project_paths']['model_artifact'])
        
        if model_path.exists() and not force_retrain:
            logger.info(f"📂 Caricamento modello esistente da {model_path}...")
            self.predictor.load_model(str(model_path))
            logger.info("✅ Modello caricato con successo.")
            return

        logger.info("Training di un nuovo modello...")
        feature_engineer = FeatureEngineering(
            self.config['ml_params'], 
            dem_path=self.pipeline_state['aux_data'].get('dem_path')
        )
        self.predictor.feature_engineer = feature_engineer
        
        X, y = self.predictor.prepare_training_data(self.pipeline_state['events_gdf'])
        metrics = self.predictor.train(X, y)
        self.predictor.save_model(str(model_path))
        self._generate_training_report(metrics)
        logger.info(f"✅ Step 2 completato: Modello addestrato e salvato.")

    def step3_make_predictions(self):
        " Genera predizioni sulla griglia e le arricchisce con il post-processor."
        logger.info("="*25 + " PREDIZIONI " + "="*25)
        grid_res = self.config['ml_params']['prediction']['grid_resolution_deg']
        bounds = self.config['ml_params']['prediction']['lombardy_bounds']
        
        lats = np.arange(bounds['lat_min'], bounds['lat_max'], grid_res)
        lons = np.arange(bounds['lon_min'], bounds['lon_max'], grid_res)
        grid_points = [(lat, lon) for lat in lats for lon in lons]
        
        predictions_raw_df = self.predictor.predict(grid_points)
        
        # Filtra e converte in GeoDataFrame
        threshold = self.config['ml_params']['prediction']['min_risk_score_threshold']
        predictions_filtered_df = predictions_raw_df[predictions_raw_df['risk_score'] >= threshold]
        
        predictions_gdf = gpd.GeoDataFrame(
            predictions_filtered_df,
            geometry=gpd.points_from_xy(predictions_filtered_df.longitude, predictions_filtered_df.latitude),
            crs='EPSG:4326'
        )
        
        # Arricchisce i dati usando il post-processor
        self.pipeline_state['predictions_gdf'] = self.post_processor.enrich_predictions(predictions_gdf)
        
        # Salva le predizioni finali
        output_path = Path(self.config['project_paths']['predictions']) / f'predictions_{datetime.now().strftime("%Y%m%d")}.geojson'
        self.pipeline_state['predictions_gdf'].to_file(output_path, driver='GeoJSON')
        
        logger.info(f"✅ Step 3 completato: {len(self.pipeline_state['predictions_gdf'])} allerte generate e salvate in {output_path}.")

    def step4_publish_results(self):
        "Step 4: Pubblica i risultati finali per il frontend."
        logger.info("="*25 + " PUBBLICAZIONE " + "="*25)
        if 'predictions_gdf' not in self.pipeline_state or self.pipeline_state['predictions_gdf'].empty:
            logger.warning("Nessuna predizione. Step saltato.")
            return
            
        self.publisher.publish_geodataframe(
            self.pipeline_state['predictions_gdf'],
            "Georisk Sentinel Lombardia - ML Predictions"
        )
        logger.info(f"✅ Step 4 completato: Risultati pubblicati nella cartella frontend.")

    def run_full_pipeline(self, force_retrain: bool = False):
        """Esegue il pipeline completo in sequenza."""
        logger.info("🚀" * 20 + " AVVIO PIPELINE COMPLETO " + "🚀" * 20)
        start_time = datetime.now()
        
        try:
            self.step1_download_data()
            self.step2_train_model(force_retrain)
            self.step3_make_predictions()
            self.step4_publish_results()
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ PIPELINE COMPLETATO CON SUCCESSO in {duration:.1f} secondi.")
            absolute_path = Path('index.html').resolve()
            logger.info(f"🌐 Visualizza risultati: file://{absolute_path}")
        except Exception as e:
            logger.exception("❌ ERRORE CRITICO: Il pipeline è stato interrotto.")
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Pipeline fallito dopo {duration:.1f} secondi.")
    #generazione report del training leggibile
    def _generate_training_report(self, metrics: Dict):
        "Genera un report HTML del training usando Jinja2."
        if not self.jinja_env:
            logger.info("Engine Jinja2 non disponibile, report di training saltato.")
            return

        try:
            template = self.jinja_env.get_template('training_report.html')
            
            feature_importances = self.predictor.get_feature_importance().head(10).to_dict('records')
            
            html_content = template.render(
                timestamp=datetime.now().strftime('%Y-%m-%d %H:%M'),
                model_type=self.predictor.model_type.upper(),
                metrics=metrics,
                features=feature_importances
            )
            
            report_path = Path(self.config['project_paths']['reports']) / f'training_{datetime.now().strftime("%Y%m%d_%H%M")}.html'
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(html_content, encoding='utf-8')
            logger.info(f"📊 Report di training salvato: {report_path}")
        except Exception as e:
            logger.error(f"Impossibile generare il report di training: {e}")

def main():
    "Funzione principale per l'esecuzione da linea di comando."
    parser = argparse.ArgumentParser(description='Georisk Sentinel ML Pipeline.')
    parser.add_argument('--step', type=int, help='Esegui solo uno step specifico (1-4)')
    parser.add_argument('--force-retrain', action='store_true', help='Forza il re-training del modello.')
    
    args = parser.parse_args()
    
    try:
        pipeline = MLPipeline("config.json")
    except ValueError as e:
        logger.error(f"Impossibile avviare il pipeline: {e}")
        sys.exit(1)

    if args.step:
        try:
            if args.step == 1: pipeline.step1_download_data()
            elif args.step == 2:
                pipeline.step1_download_data() 
                pipeline.step2_train_model(args.force_retrain)
            elif args.step == 3: 
                pipeline.step1_download_data()
                pipeline.step2_train_model()
                pipeline.step3_make_predictions()
            elif args.step == 4:
                pipeline.step1_download_data()
                pipeline.step2_train_model()
                pipeline.step3_make_predictions()
                pipeline.step4_publish_results()
            else:
                logger.error(f"Step '{args.step}' non valido. Scegli tra 1 e 4.")
        except Exception as e:
            logger.error(f"Errore durante l'esecuzione dello Step {args.step}: {e}")
            sys.exit(1)
    else:
        pipeline.run_full_pipeline(args.force_retrain)

if __name__ == "__main__":
    main()