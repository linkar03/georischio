import json
import geopandas as gpd
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class DataExporter:
    """Pubblica le predizioni come JSON statico per il frontend."""
    
    def __init__(self, output_folder: str = "frontend/data"):
        self.output_folder = Path(output_folder)
        self.output_folder.mkdir(parents=True, exist_ok=True)

    def export_geodataframe(self, gdf: gpd.GeoDataFrame, layer_title: str = None) -> dict:
        """
        Converte GeoDataFrame in JSON e lo salva per il frontend.
        
        Args:
            gdf: GeoDataFrame con le predizioni
            layer_title: Titolo del layer
            
        Returns:
            Dict con info sulla pubblicazione
        """
        logger.info("Pubblicazione predizioni...")
        
        if not isinstance(gdf, gpd.GeoDataFrame):
            raise ValueError("Input deve essere un GeoDataFrame")
        
        # Prepara i dati
        dati = self._prepare_data(gdf, layer_title)
        
        # Salva JSON principale
        file_json = self.output_folder / "alerts_data.json"
        with open(file_json, 'w', encoding='utf-8') as f:
            json.dump(dati, f, indent=2, ensure_ascii=False)
        
        # Salva anche GeoJSON per eventuali usi futuri
        file_geojson = self.output_folder / "current_alerts.geojson"
        gdf.to_file(file_geojson, driver='GeoJSON')
        
        logger.info(f"✅ Pubblicazione completata: {len(gdf)} allerte")
        
        return {
            "successo": True,
            "timestamp": datetime.now().isoformat(),
            "numero_allerte": len(gdf),
            "files": {
                "json": str(file_json),
                "geojson": str(file_geojson)
            }
        }

    def _prepare_data(self, gdf: gpd.GeoDataFrame, title: str) -> dict:
        """Prepara struttura dati per il frontend."""
        
        if gdf.empty:
            return self._get_empty_data_structure(title)
        
        # Conta allerte per livello
        conteggi = gdf['alert_level'].value_counts()
        
        # Prepara lista allerte
        allerte = []
        for _, riga in gdf.iterrows():
            try:
                lon, lat = riga.geometry.x, riga.geometry.y
                allerte.append({
                    "comune": str(riga.get('comune', 'N/A')),
                    "provincia": str(riga.get('provincia', 'N/A')),
                    "lat": round(float(lat), 4),
                    "lon": round(float(lon), 4),
                    "alert_level": str(riga.get('alert_level', 'VERDE')),
                    "alert_color": str(riga.get('alert_color', '#26de81')),
                    "risk_score": round(float(riga.get('risk_score', 0)), 1),
                    "precipitation_mm": round(float(riga.get('precipitation_mm', 0)), 1)
                })
            except Exception as e:
                logger.warning(f"Errore riga: {e}")
                continue
        
        # Ordina per rischio decrescente
        allerte.sort(key=lambda x: x["risk_score"], reverse=True)
        
        return {
            "metadata": {
                "title": title or "Georisk Sentinel Lombardia",
                "timestamp": datetime.now().isoformat(),
                "version": "2.0.0"
            },
            "summary": {
                "total": len(allerte),
                "red": int(conteggi.get("ROSSO", 0)),
                "orange": int(conteggi.get("ARANCIONE", 0)),
                "yellow": int(conteggi.get("GIALLO", 0)),
                "green": int(conteggi.get("VERDE", 0))
            },
            "alerts": allerte,
            "critical_areas": [a for a in allerte if a['alert_level'] in ['ROSSO', 'ARANCIONE']]
        }
    
    def _get_empty_data_structure(self, title: str) -> dict:
        """Struttura dati vuota ma valida."""
        return {
            "metadata": {
                "title": title or "Georisk Sentinel Lombardia",
                "timestamp": datetime.now().isoformat(),
                "version": "2.0.0"
            },
            "summary": {"total": 0, "red": 0, "orange": 0, "yellow": 0, "green": 0},
            "alerts": [],
            "critical_areas": []
        }