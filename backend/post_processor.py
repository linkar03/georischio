
"Arricchisce le predizioni grezze del modello ML con dati contestuali usando una ricerca spaziale vettorizzata per migliorare le performance"

import pandas as pd
import geopandas as gpd
from datetime import datetime
from scipy.spatial import KDTree
import numpy as np
import logging

logger = logging.getLogger(__name__)

class PredictionPostProcessor:
    def __init__(self, config: dict):
        self.config = config.get('post_processing', {})
        
        # Pre-calcola la struttura dati per la ricerca KDTree all'avvio
        capoluoghi_raw = self.config.get('capoluoghi_coords', {})
        if capoluoghi_raw:
            # Estrae nomi, coordinate e province in liste separate
            self.capoluoghi_nomi = list(capoluoghi_raw.keys())
            coords_prov = list(capoluoghi_raw.values())
            # Converte le coordinate da (lat, lon) a (x, y) per il KDTree
            self.capoluoghi_coords = np.array([[c[1], c[0]] for c in coords_prov])
            self.capoluoghi_prov = [c[2] for c in coords_prov]
            # Costruisce l'albero spaziale
            self.kdtree = KDTree(self.capoluoghi_coords)
        else:
            self.kdtree = None
            logger.warning("Dati dei capoluoghi non trovati nella configurazione.")

    def _enrich_locations(self, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
        "Usa KDTree per trovare comune e provincia per tutti i punti in un colpo solo."
        if not self.kdtree or gdf.empty:
            return gdf.assign(comune="Unknown", provincia="N/A")

        # Estrae le coordinate (lon, lat) dal GeoDataFrame
        points_to_query = np.array([gdf.geometry.x, gdf.geometry.y]).T
        
        # Esegue la query: trova l'indice del vicino più prossimo per ogni punto
        distances, indices = self.kdtree.query(points_to_query, k=1)
        
        # Mappa gli indici ai nomi dei comuni e delle province
        gdf['comune'] = [self.capoluoghi_nomi[i] for i in indices]
        gdf['provincia'] = [self.capoluoghi_prov[i] for i in indices]
        
        return gdf

    def enrich_predictions(self, predictions_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        "Arricchisce predizioni "
        if predictions_gdf.empty:
            logger.info("GeoDataFrame vuoto. Ritorno senza modifiche.")
            return predictions_gdf

        gdf = predictions_gdf.copy()
        
        try:
            # Aggiungi informazioni geografiche
            gdf = self._enrich_locations(gdf)
            
            # Calcola livello di rischio
            risk_conditions = [
                (gdf['risk_score'] >= 70, 'R4'),
                (gdf['risk_score'] >= 50, 'R3'),
                (gdf['risk_score'] >= 30, 'R2'),
                (gdf['risk_score'] < 30, 'R1')
            ]
            gdf['risk_level'] = np.select([cond for cond, _ in risk_conditions], 
                                        [val for _, val in risk_conditions], 
                                        default='R1')
            
            # Aggiungi colori con fallback
            color_map = self.config.get('color_map', {
                'ROSSO': '#ff4757',
                'ARANCIONE': '#ff9f43', 
                'GIALLO': '#ffd32c',
                'VERDE': '#26de81'
            })
            
            gdf['alert_color'] = gdf['alert_level'].map(color_map).fillna('#26de81')
            
            # Campi derivati con validazione
            gdf['precipitation_mm'] = np.clip(gdf['risk_score'] * 0.8, 0, 100).round(1)
            gdf['timestamp'] = datetime.now().isoformat()
            
            # Validazione finale
            required_columns = ['comune', 'provincia', 'alert_level', 'alert_color', 'risk_level']
            missing_cols = [col for col in required_columns if col not in gdf.columns]
            if missing_cols:
                logger.error(f"Colonne mancanti dopo enrichment: {missing_cols}")
            
            logger.info(f"Enrichment completato: {len(gdf)} records processati.")
            return gdf
            
        except Exception as e:
            logger.error(f"Errore durante enrichment: {e}")
            # Restituisce il dataframe originale con campi minimi
            gdf['comune'] = 'Unknown'
            gdf['provincia'] = 'N/A'
            gdf['alert_color'] = '#26de81'
            gdf['risk_level'] = 'R1'
            return gdf