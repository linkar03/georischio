import json
import requests
import geopandas as gpd
import pandas as pd
import rasterio
import zipfile
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, Tuple
import json
from dotenv import load_dotenv
from shapely.geometry import Point, box, LineString
import numpy as np
from rasterio.transform import from_bounds

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class DataDownloader:
    """Classe per scaricare dati, con logica di caching, fallback e sicurezza."""
    
    def __init__(self, config: Dict):
        """Inizializza con una configurazione esterna."""
        self.config = config
        raw_data_path = config.get('project_paths', {}).get('raw_data', 'data/raw')
        self.data_dir = Path(raw_data_path)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.cache_duration = config.get('data_ingestion', {}).get('cache_duration_days', 7)
        self.endpoints = config.get('data_ingestion', {}).get('endpoints', {})
        self.wfs_params = config.get('data_ingestion', {}).get('wfs_params', {})

    def _is_cache_valid(self, file_path: Path) -> bool:
        """Controlla se un file in cache è valido basandosi sulla sua età."""
        if not file_path.exists():
            return False
        
        file_mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if (datetime.now() - file_mod_time).days < self.cache_duration:
            logger.info(f"Cache valida trovata per {file_path.name}.")
            return True
        
        logger.info(f"Cache scaduta per {file_path.name}. Verrà rigenerato/riscaricato.")
        return False

    def _secure_extract_zip(self, zip_path: Path, extract_dir: Path):
        """Estrae un file ZIP in modo sicuro, prevenendo attacchi Zip Slip."""
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.infolist():
                target_path = (extract_dir / member.filename).resolve()
                if not str(target_path).startswith(str(extract_dir.resolve())):
                    logger.error(f"SECURITY ALERT: Tentativo di Zip Slip rilevato in {zip_path} per il file {member.filename}")
                    continue
                
                if not member.is_dir():
                    zip_ref.extract(member, extract_dir)
        logger.info(f"Archivio {zip_path.name} estratto in modo sicuro.")

    # Generazione dati sintetici realistici per fallback e demo essenziali al momento. Generati artificialmente
    def _generate_synthetic_iffi(self) -> gpd.GeoDataFrame:
        """Genera dati IFFI realistici per la Lombardia"""
        logger.info("📋 Generazione dati IFFI realistici per demo...")
        
        # Eventi in zone montuose 
        events_data = [
            # Valtellina (zona ad alto rischio)
            {'comune': 'Bormio', 'lat': 46.466, 'lon': 10.370, 'data_evento': '2023-07-25', 'tipo_evento': 'frana_profonda', 'provincia': 'SO'},
            {'comune': 'Livigno', 'lat': 46.538, 'lon': 10.135, 'data_evento': '2023-06-15', 'tipo_evento': 'frana_superficiale', 'provincia': 'SO'},
            {'comune': 'Chiesa in Valmalenco', 'lat': 46.267, 'lon': 9.851, 'data_evento': '2023-08-10', 'tipo_evento': 'colata_detritica', 'provincia': 'SO'},
            
            # Val Camonica
            {'comune': 'Ponte di Legno', 'lat': 46.259, 'lon': 10.509, 'data_evento': '2023-07-18', 'tipo_evento': 'frana_superficiale', 'provincia': 'BS'},
            {'comune': 'Edolo', 'lat': 46.173, 'lon': 10.327, 'data_evento': '2023-09-05', 'tipo_evento': 'crollo', 'provincia': 'BS'},
            
            # Valli Bergamasche
            {'comune': 'Valbondione', 'lat': 46.047, 'lon': 10.003, 'data_evento': '2023-06-20', 'tipo_evento': 'frana_profonda', 'provincia': 'BG'},
            {'comune': 'Branzi', 'lat': 46.003, 'lon': 9.762, 'data_evento': '2023-10-15', 'tipo_evento': 'scivolamento', 'provincia': 'BG'},
            
            # Lago di Como
            {'comune': 'Bellano', 'lat': 46.041, 'lon': 9.301, 'data_evento': '2023-11-10', 'tipo_evento': 'crollo', 'provincia': 'LC'},
            {'comune': 'Varenna', 'lat': 46.010, 'lon': 9.285, 'data_evento': '2023-10-25', 'tipo_evento': 'frana_superficiale', 'provincia': 'LC'},
            
            # Zona pianura (eventi minori)
            {'comune': 'Lodi', 'lat': 45.314, 'lon': 9.502, 'data_evento': '2023-10-15', 'tipo_evento': 'alluvione', 'provincia': 'LO'},
            {'comune': 'Cremona', 'lat': 45.133, 'lon': 10.022, 'data_evento': '2023-11-20', 'tipo_evento': 'esondazione', 'provincia': 'CR'},
        ]
        
        geometries = [Point(event['lon'], event['lat']) for event in events_data]
        gdf = gpd.GeoDataFrame(events_data, geometry=geometries, crs='EPSG:4326')
        
        # Intensità basata su tipo evento
        intensita_map = {
            'frana_superficiale': 40,
            'frana_profonda': 75,
            'crollo': 85,
            'scivolamento': 60,
            'colata_detritica': 70,
            'alluvione': 55,
            'esondazione': 45
        }
        gdf['intensita'] = gdf['tipo_evento'].map(intensita_map)
        
        return gdf

    def _generate_realistic_dem(self, output_file: Path) -> str:
        """Genera un DEM realistico per la Lombardia"""
        logger.info("📋 Generazione DEM realistico per la Lombardia...")
        
        # Dimensioni e bounds corretti per la Lombardia
        width, height = 200, 150
        west, south, east, north = 8.5, 45.4, 11.4, 46.6
        
        # griglia di coordinate
        lons = np.linspace(west, east, width)
        lats = np.linspace(south, north, height)
        lon_grid, lat_grid = np.meshgrid(lons, lats)
        
        # Genera elevazione realistica
        # Base: pianura padana bassa, montagne a nord
        elevation = np.zeros((height, width))
        
        # Pianura Padana (sud della regione)
        elevation[:50, :] = 100 + np.random.normal(0, 10, (50, width))
        
        # Prealpi (centro)
        elevation[50:100, :] = 500 + (lat_grid[50:100, :] - 45.8) * 2000 + np.random.normal(0, 50, (50, width))
        
        # Alpi (nord)
        elevation[100:, :] = 1500 + (lat_grid[100:, :] - 46.2) * 3000 + np.random.normal(0, 100, (50, width))
        
        # Aggiungi delle valli (corridoi più bassi)
        for i in range(5):
            valley_x = np.random.randint(20, width-20)
            elevation[50:, valley_x-2:valley_x+2] *= 0.7
        
        # filtra per rendere più naturale
        from scipy.ndimage import gaussian_filter
        elevation = gaussian_filter(elevation, sigma=1.5)
        
        # Assicura valori positivi
        elevation = np.maximum(elevation, 50)
        
        # Crea la trasformazione affine
        transform = from_bounds(west, south, east, north, width, height)
        
        # Salva come GeoTIFF
        with rasterio.open(
            output_file, 'w',
            driver='GTiff',
            height=height,
            width=width,
            count=1,
            dtype=elevation.dtype,
            crs='EPSG:4326',
            transform=transform,
            nodata=-9999
        ) as dst:
            dst.write(elevation.astype(np.float32), 1)
        
        logger.info(f"✅ DEM realistico generato: {output_file}")
        return str(output_file)

    def _generate_synthetic_landuse(self) -> gpd.GeoDataFrame:
        """Genera uso del suolo realistico"""
        logger.info("📋 Generazione dati uso suolo realistici...")
        landuse_data = []
        
        # Crea una griglia più dettagliata
        for lat in np.arange(45.4, 46.6, 0.05):
            for lon in np.arange(8.5, 11.4, 0.05):
                # Logica realistica basata su altitudine
                if lat > 46.3:  # Montagne
                    landuse = np.random.choice(['forest', 'rock', 'snow'], p=[0.6, 0.3, 0.1])
                elif lat > 45.9:  # Colline
                    landuse = np.random.choice(['forest', 'agricultural', 'urban'], p=[0.5, 0.4, 0.1])
                else:  # Pianura
                    landuse = np.random.choice(['agricultural', 'urban', 'industrial'], p=[0.6, 0.3, 0.1])
                
                landuse_data.append({
                    'geometry': box(lon, lat, lon + 0.05, lat + 0.05),
                    'landuse_class': landuse,
                    'risk_factor': {'forest': 0.3, 'agricultural': 0.2, 'urban': 0.7, 
                                   'industrial': 0.5, 'rock': 0.8, 'snow': 0.6}[landuse]
                })
        
        return gpd.GeoDataFrame(landuse_data, crs='EPSG:4326')

    def _generate_synthetic_rivers(self) -> gpd.GeoDataFrame:
        "Genera reticolo idrografico realistico"
        logger.info("📋 Generazione reticolo idrografico realistico...")
        
        rivers_data = [
            # Fiumi principali
            {'name': 'Po', 'order': 1, 'coords': [(8.7, 45.1), (9.5, 45.1), (10.5, 45.1), (11.4, 45.15)]},
            {'name': 'Adda', 'order': 1, 'coords': [(9.5, 46.4), (9.6, 46.0), (9.7, 45.5), (9.8, 45.1)]},
            {'name': 'Oglio', 'order': 1, 'coords': [(10.3, 46.3), (10.2, 45.8), (10.1, 45.4), (10.05, 45.1)]},
            {'name': 'Ticino', 'order': 1, 'coords': [(8.6, 45.9), (8.7, 45.5), (8.8, 45.2)]},
            {'name': 'Serio', 'order': 2, 'coords': [(9.9, 45.9), (9.85, 45.6), (9.8, 45.3)]},
            {'name': 'Brembo', 'order': 2, 'coords': [(9.6, 45.9), (9.65, 45.7), (9.7, 45.5)]},
            {'name': 'Mincio', 'order': 2, 'coords': [(10.7, 45.5), (10.75, 45.3), (10.8, 45.15)]},
        ]
        
        features = []
        for river in rivers_data:
            features.append({
                'geometry': LineString(river['coords']),
                'name': river['name'],
                'order': river['order'],
                'risk_influence': 0.5 * river['order']  # piu grandi sono i fiumi maggiore è l'influenza
            })
        
        return gpd.GeoDataFrame(features, crs='EPSG:4326')

    # metodi di download dati non utilizzati al momento, ma pronti per il futuro

    def download_ispra_iffi(self) -> gpd.GeoDataFrame:
        """Scarica i dati delle frane del progetto IFFI per la Lombardia tramite WFS."""
        output_file = self.data_dir / "iffi_lombardia.geojson"
        
        if self._is_cache_valid(output_file):
            return gpd.read_file(output_file)
        
        logger.info("🌍 Download dati frane IFFI da Geoportale Nazionale (ISPRA)...")
        
        wfs_url = "http://wms.pcn.minambiente.it/ogc/wfs/gbt.frame"
        params = {
            'service': 'WFS',
            'version': '1.0.0',
            'request': 'GetFeature',
            'typeName': 'gbt.frame:gbt.frame.frane',
            'outputFormat': 'application/json',
            'srsName': 'EPSG:4326',
            'cql_filter': "cod_reg = '03'"
        }
        
        #simulazione user agent
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }

        try:
            response = requests.get(wfs_url, params=params, headers=headers, timeout=30)
            # Controlla se la richiesta ha avuto successo (lancerà un'eccezione per errori 4xx o 5xx)
            response.raise_for_status() 
            
            data = response.json()
            
            # Convertiamo il GeoJSON (in formato dict) in un GeoDataFrame
            gdf = gpd.GeoDataFrame.from_features(data["features"])
            # Imposta il CRS che viene perso in questa conversione
            gdf.set_crs("EPSG:4326", inplace=True)

            if gdf.empty:
                logger.warning("Nessun dato sulle frane trovato per la Lombardia. Il servizio WFS ha risposto correttamente ma non ha dati.")
                return self._generate_synthetic_iffi()

            logger.info(f"✅ Scaricati {len(gdf)} eventi franosi reali.")
            gdf.to_file(output_file, driver='GeoJSON')
            return gdf
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Errore durante il download dei dati IFFI: {e}. Uso il fallback con dati sintetici.")
            return self._generate_synthetic_iffi()

    def download_dem_lombardia(self) -> str:
        """Genera DEM realistico per la Lombardia"""
        output_file = self.data_dir / "lombardia_dem.tif"
        
        if self._is_cache_valid(output_file):
            return str(output_file)
        
        return self._generate_realistic_dem(output_file)

    def download_landuse_dusaf(self) -> gpd.GeoDataFrame:
        """Genera uso suolo realistico"""
        output_file = self.data_dir / "dusaf_lombardia.geojson"
        
        if self._is_cache_valid(output_file):
            return gpd.read_file(output_file)
        
        gdf = self._generate_synthetic_landuse()
        gdf.to_file(output_file, driver='GeoJSON')
        return gdf

    def download_rivers_network(self) -> gpd.GeoDataFrame:
        """Genera reticolo idrografico realistico"""
        output_file = self.data_dir / "rivers_lombardia.geojson"
        
        if self._is_cache_valid(output_file):
            return gpd.read_file(output_file)
        
        gdf = self._generate_synthetic_rivers()
        gdf.to_file(output_file, driver='GeoJSON')
        return gdf


class DataIntegrator:
    """Orchestra il download e l'integrazione di tutti i dati necessari."""
    
    def __init__(self, config_path: str = "config.json"):
        """Carica la configurazione e inizializza il downloader."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            logger.warning(f"Config '{config_path}' non trovato. Uso configurazione default.")
            self.config = self._get_default_config()
        except json.JSONDecodeError as e:
            logger.error(f"Errore parsing config: {e}")
            raise
            
        self.downloader = DataDownloader(self.config)
        
        processed_path = self.config.get('project_paths', {}).get('processed_data', 'data/processed')
        self.processed_dir = Path(processed_path)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_default_config(self) -> dict:
        """Configurazione di fallback."""
        return {
            'data_ingestion': {
                'cache_duration_days': 7,
                'endpoints': {},
                'wfs_params': {}
            },
            'project_paths': {
                'raw_data': 'data/raw',
                'processed_data': 'data/processed'
            }
        }

    def prepare_training_dataset(self) -> Tuple[gpd.GeoDataFrame, Dict[str, str]]:
        """
        Orchestra il download di tutti i dati, li integra e restituisce
        il set di dati per il training.
        """
        logger.info("Avvio preparazione dataset di training...")
        
        events_gdf = self.downloader.download_ispra_iffi()
        dem_path = self.downloader.download_dem_lombardia()
        landuse_gdf = self.downloader.download_landuse_dusaf()
        rivers_gdf = self.downloader.download_rivers_network()
        
        aux_data = {
            "dem_path": dem_path,
            "landuse_path": str(self.downloader.data_dir / "dusaf_lombardia.geojson"),
            "rivers_path": str(self.downloader.data_dir / "rivers_lombardia.geojson")
        }
        
        logger.info(f"Dataset di training preparato con successo:")
        logger.info(f"  - Eventi franosi: {len(events_gdf)}")
        logger.info(f"  - DEM: {dem_path}")
        logger.info(f"  - Uso suolo: {len(landuse_gdf)} poligoni")
        logger.info(f"  - Fiumi: {len(rivers_gdf)} segmenti")
        
        return events_gdf, aux_data


if __name__ == "__main__":
    integrator = DataIntegrator()
    events, aux_data = integrator.prepare_training_dataset()
    print("\n📊 STATISTICHE DATASET FINALE:")
    print(f"Numero di eventi franosi: {len(events)}")
    print(f"Percorso DEM: {aux_data['dem_path']}")