import json
import geopandas as gpd
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict
import shutil
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FreeAccountPublisher:
    def __init__(self, output_dir: str = "frontend/data"):
        self.output_dir = Path(output_dir)
        self.backup_dir = self.output_dir / "backup"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)
        
        # Setup Jinja2
        templates_dir = Path("templates")
        if templates_dir.exists():
            self.jinja_env = Environment(loader=FileSystemLoader(str(templates_dir)))
        else:
            self.jinja_env = None
            logger.warning("Directory templates/ non trovata. Report HTML disabilitati.")

    def _prepare_publish_data(self, gdf: gpd.GeoDataFrame, layer_title: str) -> Dict:
        "Prepara dati centralizzati dal GeoDataFrame."
        logger.info("Preparazione dati per pubblicazione...")
        
        if gdf.empty:
            return self._empty_data_structure(layer_title)
        
        alert_counts = gdf['alert_level'].value_counts()
        
        alerts = []
        for _, row in gdf.iterrows():
            try:
                lon, lat = (row.geometry.x, row.geometry.y) if row.geometry else (None, None)
                alerts.append({
                    "comune": str(row.get('comune', 'N/A')),
                    "provincia": str(row.get('provincia', 'N/A')),
                    "lat": round(float(lat), 4) if lat is not None else None,
                    "lon": round(float(lon), 4) if lon is not None else None,
                    "alert_level": str(row.get('alert_level', 'VERDE')),
                    "alert_color": str(row.get('alert_color', '#26de81')),
                    "risk_score": round(float(row.get('risk_score', 0)), 1),
                    "precipitation_mm": round(float(row.get('precipitation_mm', 0)), 1),
                    "risk_level": str(row.get('risk_level', 'N/A')),
                })
            except (ValueError, AttributeError) as e:
                logger.warning(f"Errore processing riga: {e}. Riga saltata.")
                continue

        return {
            "metadata": {
                "title": layer_title or "Georisk Sentinel Lombardia",
                "description": "Sistema di monitoraggio rischio idrogeologico",
                "timestamp": datetime.now().isoformat(),
                "version": "2.1.0-fixed",
                "total_alerts": len(alerts)
            },
            "summary": {
                "total": len(alerts),
                "red": int(alert_counts.get("ROSSO", 0)),
                "orange": int(alert_counts.get("ARANCIONE", 0)),
                "yellow": int(alert_counts.get("GIALLO", 0)),
                "green": int(alert_counts.get("VERDE", 0)),
            },
            "statistics": {
                "avg_precipitation": round(float(gdf['precipitation_mm'].mean()), 1) if not gdf.empty else 0,
                "max_precipitation": round(float(gdf['precipitation_mm'].max()), 1) if not gdf.empty else 0,
                "avg_risk_score": round(float(gdf['risk_score'].mean()), 1) if not gdf.empty else 0,
                "areas_at_risk": int((gdf['risk_score'] > 50).sum()) if not gdf.empty else 0,
            },
            "alerts": sorted(alerts, key=lambda x: x["risk_score"], reverse=True),
            "critical_areas": [a for a in alerts if a['alert_level'] in ['ROSSO', 'ARANCIONE']]
        }

    def _empty_data_structure(self, layer_title: str) -> Dict:
        "Ritorna struttura dati vuota ma valida."
        return {
            "metadata": {
                "title": layer_title or "Georisk Sentinel Lombardia",
                "description": "Sistema di monitoraggio rischio idrogeologico",
                "timestamp": datetime.now().isoformat(),
                "version": "2.1.0-fixed",
                "total_alerts": 0
            },
            "summary": {"total": 0, "red": 0, "orange": 0, "yellow": 0, "green": 0},
            "statistics": {"avg_precipitation": 0, "max_precipitation": 0, "avg_risk_score": 0, "areas_at_risk": 0},
            "alerts": [],
            "critical_areas": []
        }

    def _generate_json_data(self, data: Dict) -> str:
        "Genera file JSON principale."
        logger.info("Generazione file JSON...")
        output_file = self.output_dir / "alerts_data.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"File JSON generato: {output_file}")
            return str(output_file)
        except (IOError, TypeError) as e:
            logger.error(f"Errore generazione JSON: {e}")
            raise

    def _generate_javascript_file(self, json_file_path: str) -> str:
        "Genera file JavaScript leggendo il JSON già esistente."
        logger.info("Generazione file JavaScript...")
        
        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                json_content = f.read()
            
            js_content = f"""// Georisk Sentinel Lombardia - Data File
// Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// Source: {Path(json_file_path).name}

const GEORISK_DATA = {json_content};

// Funzione di utilità per accesso sicuro ai dati
function getGeoRiskData() {{
    return GEORISK_DATA;
}}

// Export per moduli ES6 (se necessario)
if (typeof module !== 'undefined' && module.exports) {{
    module.exports = GEORISK_DATA;
}}"""
            
            output_file = self.output_dir / "alerts_data.js"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(js_content)
            
            logger.info(f"File JavaScript generato: {output_file}")
            return str(output_file)
        except (IOError, json.JSONDecodeError) as e:
            logger.error(f"Errore generazione JavaScript: {e}")
            raise

    def _generate_geojson(self, gdf: gpd.GeoDataFrame) -> str:
        """Genera file GeoJSON."""
        logger.info("Generazione file GeoJSON...")
        output_file = self.output_dir / "current_alerts.geojson"
        
        try:
            gdf.to_file(output_file, driver='GeoJSON')
            logger.info(f"File GeoJSON generato: {output_file}")
            return str(output_file)
        except Exception as e:
            logger.error(f"Errore generazione GeoJSON: {e}")
            raise

    def _generate_html_report(self, data: Dict) -> str:
        """Genera report HTML se template disponibile."""
        if not self.jinja_env:
            logger.info("Template engine non disponibile. Report HTML saltato.")
            return ""
            
        try:
            template = self.jinja_env.get_template('report_template.html')
            render_data = data.copy()
            render_data['timestamp'] = datetime.fromisoformat(data['metadata']['timestamp']).strftime('%d/%m/%Y alle %H:%M')
            
            html_content = template.render(render_data)
            output_file = self.output_dir / "report.html"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"Report HTML generato: {output_file}")
            return str(output_file)
        except Exception as e:
            logger.error(f"Errore generazione report HTML: {e}")
            return ""
    
    def _backup_existing_files(self):
        """Backup sicuro dei file esistenti."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        files_to_backup = ["alerts_data.json", "alerts_data.js", "current_alerts.geojson"]
        
        for filename in files_to_backup:
            source = self.output_dir / filename
            if source.exists():
                try:
                    dest = self.backup_dir / f"{filename}.{timestamp}.bak"
                    shutil.copy2(source, dest)
                    logger.debug(f"Backup creato: {dest}")
                except IOError as e:
                    logger.warning(f"Errore backup {filename}: {e}")

    def publish_geodataframe(self, gdf: gpd.GeoDataFrame, layer_title: str = None) -> Dict:
        logger.info("Inizio processo di pubblicazione...")
        
        try:
            # Validazione input
            if not isinstance(gdf, gpd.GeoDataFrame):
                raise ValueError("Input deve essere un GeoDataFrame")
            
            self._backup_existing_files()
            
            #PREPARA I DATI (Single Source of Truth)
            publish_data = self._prepare_publish_data(gdf, layer_title)
            
            #GENERA I FILE (ogni file una volta sola)
            json_file = self._generate_json_data(publish_data)
            js_file = self._generate_javascript_file(json_file)  # Usa il path del JSON
            geojson_file = self._generate_geojson(gdf)
            html_file = self._generate_html_report(publish_data)
            
            files_generated = {
                "json": json_file,
                "javascript": js_file,
                "geojson": geojson_file,
                "report": html_file if html_file else None
            }

            logger.info("Pubblicazione completata con successo!")
            return {
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "files_generated": files_generated,
                "alerts_count": publish_data["metadata"]["total_alerts"]
            }
            
        except Exception as e:
            logger.exception("Errore durante la pubblicazione.")
            return {
                "success": False, 
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }