import logging
from datetime import datetime

import geopandas as gpd
import numpy as np
from scipy.spatial import KDTree

logger = logging.getLogger(__name__)

class PredictionPostProcessor:
    def __init__(self, config: dict):
        self.config = config.get('post_processing', {})
        
        capitals_raw = self.config.get('capoluoghi_coords', {})
        if capitals_raw:
            self.capital_names = list(capitals_raw.keys())
            coords_prov = list(capitals_raw.values())
            # Converte le coordinate da (lat, lon) a (lon, lat) per coerenza spaziale
            self.capital_coords = np.array([[c[1], c[0]] for c in coords_prov])
            self.capital_provinces = [c[2] for c in coords_prov]
            self.kdtree = KDTree(self.capital_coords)
        else:
            self.kdtree = None
            logger.warning("Dati dei capoluoghi non trovati nella configurazione. L'arricchimento della località sarà limitato.")

    def _enrich_locations(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Usa un KDTree per associare efficientemente ogni punto al comune e provincia più vicini."""
        if not self.kdtree or gdf.empty:
            return gdf.assign(comune="Unknown", provincia="N/A")

        points_to_query = np.array([gdf.geometry.x, gdf.geometry.y]).T
        _, indices = self.kdtree.query(points_to_query, k=1)
        
        gdf['comune'] = [self.capital_names[i] for i in indices]
        gdf['provincia'] = [self.capital_provinces[i] for i in indices]
        
        return gdf

    def enrich_predictions(self, predictions_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """Flusso principale per arricchire un GeoDataFrame di predizioni."""
        if predictions_gdf.empty:
            logger.info("GeoDataFrame delle predizioni vuoto. Nessun arricchimento necessario.")
            return predictions_gdf

        gdf = predictions_gdf.copy()
        
        try:
            gdf = self._enrich_locations(gdf)
            
            color_map = self.config.get('color_map', {
                'ROSSO': '#ff4757', 'ARANCIONE': '#ff9f43', 
                'GIALLO': '#ffd32c', 'VERDE': '#26de81'
            })
            gdf['alert_color'] = gdf['alert_level'].map(color_map).fillna('#26de81')
            
            gdf['precipitation_mm'] = np.clip(gdf['risk_score'] * 0.8, 0, 100).round(1)
            gdf['timestamp'] = datetime.now().isoformat()
            
            logger.info(f"Arricchimento completato. {len(gdf)} record processati.")
            return gdf
            
        except Exception as e:
            logger.error(f"Errore imprevisto durante l'arricchimento delle predizioni: {e}")
            # In caso di errore, restituisce il GDF originale per non bloccare la pipeline
            return predictions_gdf