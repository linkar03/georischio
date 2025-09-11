import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import geopandas as gpd
import numpy as np
import rasterio
import requests
from dotenv import load_dotenv
from rasterio.transform import from_bounds
from scipy.ndimage import gaussian_filter
from shapely.geometry import Point, box, LineString

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

class DataDownloader:
    """Gestisce il download e il caching dei dati da fonti esterne."""

    def __init__(self, config: Dict):
        self.config = config
        raw_data_path = config.get('project_paths', {}).get('raw_data', 'data/raw')
        self.data_dir = Path(raw_data_path)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_duration_days = config.get('data_ingestion', {}).get('cache_duration_days', 7)

    def _is_cache_valid(self, file_path: Path) -> bool:
        """Controlla se un file in cache è valido basandosi sulla sua età."""
        if not file_path.exists():
            return False
        
        file_mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        if (datetime.now() - file_mod_time).days < self.cache_duration_days:
            logger.info(f"Cache valida trovata per {file_path.name}.")
            return True
        
        logger.info(f"Cache scaduta per {file_path.name}. Il file verrà rigenerato/riscaricato.")
        return False

    def _generate_synthetic_events(self) -> gpd.GeoDataFrame:
        """Genera dati IFFI sintetici ma realistici per fallback o demo."""
        logger.info("Generazione dati IFFI sintetici di fallback...")
        events_data = [
            {'comune': 'Bormio', 'lat': 46.466, 'lon': 10.370, 'data_evento': '2023-07-25', 'tipo_evento': 'frana_profonda', 'provincia': 'SO'},
            {'comune': 'Livigno', 'lat': 46.538, 'lon': 10.135, 'data_evento': '2023-06-15', 'tipo_evento': 'frana_superficiale', 'provincia': 'SO'},
            {'comune': 'Chiesa in Valmalenco', 'lat': 46.267, 'lon': 9.851, 'data_evento': '2023-08-10', 'tipo_evento': 'colata_detritica', 'provincia': 'SO'},
            {'comune': 'Ponte di Legno', 'lat': 46.259, 'lon': 10.509, 'data_evento': '2023-07-18', 'tipo_evento': 'frana_superficiale', 'provincia': 'BS'},
            {'comune': 'Bellano', 'lat': 46.041, 'lon': 9.301, 'data_evento': '2023-11-10', 'tipo_evento': 'crollo', 'provincia': 'LC'},
        ]
        gdf = gpd.GeoDataFrame(events_data, geometry=[Point(e['lon'], e['lat']) for e in events_data], crs='EPSG:4326')
        intensita_map = {'frana_superficiale': 40, 'frana_profonda': 75, 'colata_detritica': 70, 'crollo': 85}
        gdf['intensita'] = gdf['tipo_evento'].map(intensita_map)
        return gdf

    def _generate_synthetic_dem(self, output_file: Path) -> str:
        """Genera un DEM (Digital Elevation Model) sintetico ma realistico per la Lombardia."""
        logger.info(f"Generazione DEM sintetico: {output_file}...")
        width, height = 200, 150
        bounds = (8.5, 45.4, 11.4, 46.6)  # west, south, east, north
        transform = from_bounds(*bounds, width, height)
        lons = np.linspace(bounds[0], bounds[2], width)
        lats = np.linspace(bounds[1], bounds[3], height)
        lon_grid, lat_grid = np.meshgrid(lons, lats)
        
        elevation = 100 + (lat_grid - 45.4) * 1500 + np.random.normal(0, 50, (height, width))
        elevation = gaussian_filter(elevation, sigma=1.5)
        elevation = np.maximum(elevation, 50)
        
        with rasterio.open(output_file, 'w', driver='GTiff', height=height, width=width, count=1,
                           dtype=elevation.dtype, crs='EPSG:4326', transform=transform, nodata=-9999) as dst:
            dst.write(elevation.astype(np.float32), 1)
        return str(output_file)

    def _generate_synthetic_landuse(self) -> gpd.GeoDataFrame:
        """Genera dati di uso del suolo (land use) sintetici."""
        logger.info("Generazione dati uso del suolo sintetici...")
        landuse_data = []
        for lat in np.arange(45.4, 46.6, 0.1):
            for lon in np.arange(8.5, 11.4, 0.1):
                if lat > 46.2: landuse_class = 'forest'
                elif lat < 45.7: landuse_class = 'urban'
                else: landuse_class = 'agricultural'
                landuse_data.append({'geometry': box(lon, lat, lon + 0.1, lat + 0.1), 'landuse_class': landuse_class})
        return gpd.GeoDataFrame(landuse_data, crs='EPSG:4326')
    
    def _generate_synthetic_rivers(self) -> gpd.GeoDataFrame:
        """Genera un reticolo idrografico sintetico."""
        logger.info("Generazione reticolo idrografico sintetico...")
        rivers_data = [
            {'name': 'Po', 'coords': [(8.7, 45.1), (11.4, 45.15)]},
            {'name': 'Adda', 'coords': [(9.5, 46.4), (9.8, 45.1)]},
        ]
        features = [{'geometry': LineString(r['coords']), 'name': r['name']} for r in rivers_data]
        return gpd.GeoDataFrame(features, crs='EPSG:4326')

    def fetch_landslide_events(self) -> gpd.GeoDataFrame:
        """Scarica i dati delle frane IFFI via WFS, con fallback su dati sintetici."""
        output_file = self.data_dir / "iffi_lombardia.geojson"
        if self._is_cache_valid(output_file):
            return gpd.read_file(output_file)
        
        logger.info("Download dati frane IFFI da Geoportale Nazionale (ISPRA)...")
        wfs_url = "http://wms.pcn.minambiente.it/ogc/wfs/gbt.frame"
        params = {'service': 'WFS', 'version': '1.0.0', 'request': 'GetFeature',
                  'typeName': 'gbt.frame:gbt.frame.frane', 'outputFormat': 'application/json',
                  'srsName': 'EPSG:4326', 'cql_filter': "cod_reg = '03'"}
        headers = {'User-Agent': 'Georisk-Analysis-Tool/1.0'}
        try:
            response = requests.get(wfs_url, params=params, headers=headers, timeout=45)
            response.raise_for_status()
            gdf = gpd.GeoDataFrame.from_features(response.json()["features"], crs="EPSG:4326")
            if gdf.empty:
                raise ValueError("Nessun dato IFFI restituito dal servizio WFS.")
            logger.info(f"Scaricati {len(gdf)} eventi franosi reali.")
            gdf.to_file(output_file, driver='GeoJSON')
            return gdf
        except (requests.exceptions.RequestException, ValueError, KeyError) as e:
            logger.error(f"Errore download dati IFFI: {e}. Uso fallback con dati sintetici.")
            return self._generate_synthetic_events()

    def fetch_dem(self) -> str:
        """Fornisce il percorso a un DEM, generandone uno sintetico se non disponibile."""
        output_file = self.data_dir / "lombardia_dem.tif"
        if self._is_cache_valid(output_file):
            return str(output_file)
        return self._generate_synthetic_dem(output_file)

    def fetch_landuse(self) -> gpd.GeoDataFrame:
        """Fornisce i dati di uso del suolo, generando dati sintetici se non disponibili."""
        output_file = self.data_dir / "landuse_lombardia.geojson"
        if self._is_cache_valid(output_file):
            return gpd.read_file(output_file)
        gdf = self._generate_synthetic_landuse()
        gdf.to_file(output_file, driver='GeoJSON')
        return gdf

    def fetch_rivers(self) -> gpd.GeoDataFrame:
        """Fornisce il reticolo idrografico, generando dati sintetici se non disponibili."""
        output_file = self.data_dir / "rivers_lombardia.geojson"
        if self._is_cache_valid(output_file):
            return gpd.read_file(output_file)
        gdf = self._generate_synthetic_rivers()
        gdf.to_file(output_file, driver='GeoJSON')
        return gdf

class DataIntegrator:
    """Orchestra il download e la preparazione di tutti i dati necessari per il modello."""

    def __init__(self, config_path: str = "config.json"):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Errore caricamento config: {e}. Funzionalità limitate.")
            self.config = {}
        self.downloader = DataDownloader(self.config)

    def prepare_training_dataset(self) -> Tuple[gpd.GeoDataFrame, Dict[str, str]]:
        """
        Coordina il recupero di tutti i dati e restituisce il dataset per il training
        e un dizionario con i percorsi ai dati ausiliari.
        """
        logger.info("Avvio preparazione dataset di training...")
        
        events_gdf = self.downloader.fetch_landslide_events()
        dem_path = self.downloader.fetch_dem()
        landuse_gdf = self.downloader.fetch_landuse()
        rivers_gdf = self.downloader.fetch_rivers()
        
        aux_data = {
            "dem_path": dem_path,
            "landuse_path": str(self.downloader.data_dir / "landuse_lombardia.geojson"),
            "rivers_path": str(self.downloader.data_dir / "rivers_lombardia.geojson")
        }
        
        logger.info("Dataset di training preparato con successo.")
        logger.info(f"  - Eventi franosi: {len(events_gdf)}")
        logger.info(f"  - DEM: {dem_path}")
        logger.info(f"  - Uso suolo: {len(landuse_gdf)} poligoni")
        logger.info(f"  - Fiumi: {len(rivers_gdf)} segmenti")
        
        return events_gdf, aux_data