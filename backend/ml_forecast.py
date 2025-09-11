import warnings
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.mask import mask
import requests
import xgboost as xgb
from scipy import ndimage
from shapely.geometry import Point, mapping
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

# configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
warnings.filterwarnings('ignore', category=UserWarning, module='geopandas')

# Gestione import opzionali - non critici
CACHE_AVAILABLE = False
try:
    import requests_cache
    requests_cache.install_cache('georisk_api_cache', backend='sqlite', expire_after=3600)
    CACHE_AVAILABLE = True
    logger.info("Cache delle richieste API attivata (dati salvati per 1 ora).")
except ImportError:
    logger.info("requests_cache non disponibile. Cache disabilitata (non è un problema).")

PYDANTIC_AVAILABLE = False
try:
    from pydantic import BaseModel, Field
    PYDANTIC_AVAILABLE = True
    
    #modelli validazione dati pydantic
    class MeteoDailyData(BaseModel):
        "Valida la struttura dei dati giornalieri di Open-Meteo"
        time: List[datetime]
        precipitation_sum: List[Optional[float]] = Field(..., alias='precipitation_sum')

    class MeteoApiResponse(BaseModel):
        "Valida la struttura della risposta API di Open-Meteo."
        latitude: float
        longitude: float
        daily: MeteoDailyData
        
except ImportError:
    logger.info("Validazione dati disabilitata")


class FeatureEngineering:
    "estrazione feature e feature engineering"
    
    def __init__(self, config: Dict, dem_path: Optional[str] = None):
        self.config = config.get('feature_engineering', {})
        self.dem_path = Path(dem_path) if dem_path else None
        
    def extract_terrain_features(self, geometry: Point) -> Dict:
        "Estrae features del terreno da un DEM"
        buffer_radius_m = self.config.get('terrain_buffer_radius_m', 500)
        
        if self.dem_path and self.dem_path.exists():
            try:
                with rasterio.open(self.dem_path) as dem:
                    point_gdf = gpd.GeoDataFrame([{'geometry': geometry}], crs='EPSG:4326')
                    
                    # Proietta in UTM (zona 32N per Lombardia)
                    point_proj = point_gdf.to_crs("EPSG:32632")
                    
                    # raggio buffer in metri
                    buffer_utm = point_proj.buffer(buffer_radius_m)
                    
                    # Riporta il buffer al CRS del DEM
                    buffer_dem_crs = buffer_utm.to_crs(dem.crs)
                    
                    # Maschera il DEM con il buffer
                    geometry_to_mask = [mapping(g) for g in buffer_dem_crs.geometry]
                    
                    out_image, out_transform = mask(
                        dem, 
                        shapes=geometry_to_mask,
                        crop=True, 
                        all_touched=True
                    )

                    elevation_data = out_image[0][out_image[0] != dem.nodata]
                    if elevation_data.size < 10:
                        raise ValueError("Dati DEM insufficienti nell'area del buffer.")

                    dy, dx = np.gradient(elevation_data)
                    slope = np.sqrt(dx**2 + dy**2)
                    return {
                        'elevation_mean': float(np.mean(elevation_data)),
                        'elevation_std': float(np.std(elevation_data)),
                        'slope_mean': float(np.mean(slope)),
                        'roughness': float(np.std(elevation_data - ndimage.uniform_filter(elevation_data, size=3)))
                    }
            except (ValueError, rasterio.errors.RasterioIOError, IndexError) as e:
                logger.warning(f"Errore estrazione DEM per {geometry.wkt}: {e}. Uso fallback.")
                
        # Fallback
        lat = geometry.y
        elevation_estimate = 200 + (lat - 45.4) * 1500
        return {
            'elevation_mean': elevation_estimate,
            'elevation_std': 150,
            'slope_mean': 5 + (lat - 45.4) * 20,
            'roughness': 20
        }

    def extract_weather_features(self, lat: float, lon: float) -> Dict:
        """Estrae features meteo con gestione errori robusta."""
        fallback_data = {
            'precip_1d_past': 5.0,
            'precip_3d_past': 15.0,
            'precip_7d_past': 30.0,
            'precip_3d_forecast': 10.0
        }
        
        # Validazione coordinate
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            logger.warning(f"Coordinate non valide: ({lat}, {lon})")
            return fallback_data
        
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "precipitation_sum",
            "past_days": self.config.get('weather_past_days', 7),
            "forecast_days": self.config.get('weather_forecast_days', 3),
            "timezone": "Europe/Rome"
        }
        
        try:
            response = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=10,  # Timeout esplicito
                headers={'User-Agent': 'Georisk-Sentinel/1.0'}
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Validazione struttura response
            if 'daily' not in data or 'precipitation_sum' not in data['daily']:
                raise ValueError("Struttura response API non valida")
            
            precip_raw = data['daily']['precipitation_sum']
            precip = [float(p) if p is not None else 0.0 for p in precip_raw]
            
            num_past = self.config.get('weather_past_days', 7)
            
            if len(precip) < num_past:
                logger.warning(f"Dati meteo insufficienti ({len(precip)} < {num_past})")
                return fallback_data
            
            return {
                'precip_1d_past': precip[num_past-1],
                'precip_3d_past': sum(precip[num_past-3:num_past]),
                'precip_7d_past': sum(precip[0:num_past]),
                'precip_3d_forecast': sum(precip[num_past:])
            }
            
        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            logger.warning(f"Errore API meteo per ({lat:.3f},{lon:.3f}): {e}")
            return fallback_data

    def create_feature_vector(self, lat: float, lon: float, date: datetime) -> pd.DataFrame:
        "Crea un vettore di features completo per una data località e data."
        point = Point(lon, lat)
        features = {}
        features.update(self.extract_terrain_features(point))
        features.update(self.extract_weather_features(lat, lon))
        
        features['month'] = date.month
        features['day_of_year'] = date.timetuple().tm_yday
        features['latitude'] = lat
        features['longitude'] = lon
        
        return pd.DataFrame([features])


class RiskPredictor:
    "modello ml per predizione rischio"
    
    def __init__(self, config: Dict):
        self.config = config.get('model', {})
        self.model_type = self.config.get('type', 'xgboost')
        self.model = None
        self.scaler = StandardScaler()
        self.feature_engineer = None
        self.feature_names_ = []

    def prepare_training_data(self, historical_events: gpd.GeoDataFrame) -> Tuple[pd.DataFrame, pd.Series]:
        "preparazione dati training da dati storici"
        logger.info(f"Preparazione dati da {len(historical_events)} eventi storici...")
        X_list, y_list = [], []

        for _, event in historical_events.iterrows():
            lat, lon = event.geometry.y, event.geometry.x
            event_date = pd.to_datetime(event.get('data_evento', datetime.now()))
            features = self.feature_engineer.create_feature_vector(lat, lon, event_date)
            X_list.append(features)
            y_list.append(event.get('intensita', 50)) # intensita =target
        
        X = pd.concat(X_list, ignore_index=True)
        y = pd.Series(y_list)

        # Aggiungo campioni negativi (aree senza eventi) per bilanciare il dataset
        n_negative = len(X)
        logger.info(f"Aggiunta di {n_negative} campioni negativi casuali...")
        neg_samples = []
        ml_config = self.config if 'lombardy_bounds' in self.config else {}
        bounds = ml_config.get('lombardy_bounds', {'lat_min': 45.4, 'lat_max': 46.6, 'lon_min': 8.5, 'lon_max': 11.4})
        
        for _ in range(n_negative):
            lat = np.random.uniform(bounds['lat_min'], bounds['lat_max'])
            lon = np.random.uniform(bounds['lon_min'], bounds['lon_max'])
            date = datetime.now() - timedelta(days=np.random.randint(0, 3650))
            neg_samples.append(self.feature_engineer.create_feature_vector(lat, lon, date))

        X_negative = pd.concat(neg_samples, ignore_index=True)
        y_negative = pd.Series([0] * n_negative)

        X_final = pd.concat([X, X_negative], ignore_index=True)
        y_final = pd.concat([y, y_negative], ignore_index=True)

        self.feature_names_ = X_final.columns.tolist()
        logger.info(f"Dataset preparato: {len(X_final)} campioni, {len(self.feature_names_)} features")
        return X_final, y_final

    def train(self, X: pd.DataFrame, y: pd.Series) -> Dict:
        """Addestra il modello con gestione feature names consistente."""
        
        # Salva i nomi delle feature per uso futuro
        self.feature_names_ = X.columns.tolist()
        
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, 
            test_size=self.config.get('test_size', 0.2),
            random_state=self.config.get('random_state', 42)
        )
        
        # Scaling
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Training
        model_params = self.config.get('model_params', {
            'n_estimators': 100,
            'max_depth': 5,
            'learning_rate': 0.1
        })
        
        self.model = xgb.XGBRegressor(
            **model_params,
            random_state=self.config.get('random_state', 42)
        )
        
        logger.info(f"Training modello con parametri: {model_params}")
        self.model.fit(X_train_scaled, y_train)
        
        # Valutazione
        y_pred = self.model.predict(X_test_scaled)
        
        metrics = {
            'test_r2': float(r2_score(y_test, y_pred)),
            'test_rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
            'test_mae': float(mean_absolute_error(y_test, y_pred)),
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'feature_count': len(self.feature_names_)
        }
        
        logger.info(f"Training completato. R2={metrics['test_r2']:.3f}, RMSE={metrics['test_rmse']:.2f}")
        return metrics

    def get_feature_importance(self) -> pd.DataFrame:
        "Restituisce l'importanza delle feature in un DataFrame ordinato."
        if not self.model or not hasattr(self.model, 'feature_importances_'):
            return pd.DataFrame()
        
        importance_df = pd.DataFrame({
            'feature': self.feature_names_,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        return importance_df

    def predict(self, locations: List[Tuple[float, float]]) -> pd.DataFrame:
        "Predice il rischio per una lista di località."
        if not self.model: 
            raise RuntimeError("Modello non addestrato.")

        all_features = []
        for lat, lon in locations:
            all_features.append(self.feature_engineer.create_feature_vector(lat, lon, datetime.now()))
        
        features_df = pd.concat(all_features, ignore_index=True).reindex(columns=self.feature_names_)
        
        features_scaled = self.scaler.transform(features_df)
        risk_scores = self.model.predict(features_scaled)
        
        # Applica clipping per assicurare che il punteggio sia tra 0 e 100
        risk_scores = np.clip(risk_scores, 0, 100)
        
        results = []
        for i, (lat, lon) in enumerate(locations):
            score = risk_scores[i]
            if score >= 70: 
                alert_level = "ROSSO"
            elif score >= 50: 
                alert_level = "ARANCIONE"
            elif score >= 30: 
                alert_level = "GIALLO"
            else: 
                alert_level = "VERDE"
            
            results.append({
                'latitude': lat, 
                'longitude': lon, 
                'risk_score': score, 
                'alert_level': alert_level
            })
        
        return pd.DataFrame(results)

    def save_model(self, filepath: str):
        """Salva il modello, lo scaler e i nomi delle feature."""
        model_data = {
            'model': self.model, 
            'scaler': self.scaler, 
            'features': self.feature_names_
        }
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(model_data, filepath)
        logger.info(f"Modello salvato in: {filepath}")

    def load_model(self, filepath: str):
        """Carica un modello salvato."""
        model_data = joblib.load(filepath)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_names_ = model_data['features']
        logger.info(f"Modello caricato da: {filepath}")


# --- esecuzione di esempio ---
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("AVVIO WORKFLOW DI ESEMPIO PER MODELLO ML")
    logger.info("=" * 60)

    # Carica la configurazione
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("File 'config.json' non trovato. Esecuzione interrotta.")
        exit()

    # Inizializza le classi
    predictor = RiskPredictor(config['ml_params'])
    # In un'app reale, il dem_path verrebbe dal modulo `dati`
    feature_engineer = FeatureEngineering(config['ml_params'], dem_path=None)
    predictor.feature_engineer = feature_engineer

    # Simula eventi storici per il training
    historical_events = gpd.GeoDataFrame([
        {'geometry': Point(9.87, 46.16), 'intensita': 85, 'data_evento': '2023-07-15'},
        {'geometry': Point(9.67, 45.69), 'intensita': 40, 'data_evento': '2023-10-20'},
        {'geometry': Point(9.39, 45.85), 'intensita': 90, 'data_evento': '2023-11-05'},
    ], crs='EPSG:4326')

    # Prepara i dati e addestra il modello
    X, y = predictor.prepare_training_data(historical_events)
    metrics = predictor.train(X, y)
    
    print("\n📊 METRICHE DI PERFORMANCE DEL MODELLO:")
    print(pd.DataFrame([metrics]).to_string())

    # predizioni su una griglia
    logger.info("\n🎯 Esecuzione predizioni su una griglia per la Lombardia...")
    grid_res = config['ml_params']['prediction']['grid_resolution_deg']
    bounds = config['ml_params']['prediction'].get('lombardy_bounds', {
        'lat_min': 45.4, 'lat_max': 46.6, 'lon_min': 8.5, 'lon_max': 11.4
    })
    
    lats = np.arange(bounds['lat_min'], bounds['lat_max'], grid_res)
    lons = np.arange(bounds['lon_min'], bounds['lon_max'], grid_res)
    grid_points = [(lat, lon) for lat in lats for lon in lons]
    
    predictions_df = predictor.predict(grid_points)
    
    # Mostra le 5 aree a maggior rischio
    print("\nTOP 5 AREE A MAGGIOR RISCHIO PREVISTO:")
    print(predictions_df.sort_values('risk_score', ascending=False).head().to_string())
    
    # salvataggio modello
    predictor.save_model("models/georisk_predictor.pkl")
    
    output_gdf = gpd.GeoDataFrame(
        predictions_df, 
        geometry=gpd.points_from_xy(predictions_df.longitude, predictions_df.latitude),
        crs='EPSG:4326'
    )
    output_path = Path("data/predictions/latest_predictions.geojson")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_gdf.to_file(output_path, driver='GeoJSON')
    logger.info(f"Predizioni sulla griglia salvate in: {output_path}")