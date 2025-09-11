import os
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, send_file
from flask_cors import CORS
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carica variabili ambiente
load_dotenv(Path(__file__).parent / '.env')

# Setup percorsi
BACKEND_DIR = Path(__file__).parent
PROJECT_ROOT = BACKEND_DIR.parent
FRONTEND_DIR = PROJECT_ROOT / 'frontend'
DATA_DIR = FRONTEND_DIR / 'data'

# Inizializza Flask
app = Flask(__name__, 
           static_folder=str(FRONTEND_DIR),
           static_url_path='')

CORS(app)


@app.route('/api/config')
def get_api_key():
    "Fornisce la chiave API ArcGIS al frontend."
    api_key = os.getenv('ARCGIS_API_KEY')
    
    if not api_key:
        logger.error("ARCGIS_API_KEY non trovata nel file .env")
        return jsonify({"error": "API key non configurata"}), 500
    
    return jsonify({"apiKey": api_key})


@app.route('/api/alerts')
def get_alerts():
    """Fornisce i dati delle allerte."""
    alerts_file = DATA_DIR / 'alerts_data.json'
    
    if alerts_file.exists():
        return send_file(str(alerts_file), mimetype='application/json')
    
    # Dati demo se il file non esiste
    logger.warning("File alerts_data.json non trovato, invio dati demo")
    return jsonify({
        "metadata": {
            "title": "Georisk Sentinel - Demo",
            "timestamp": "2025-01-01T12:00:00",
            "version": "2.0.0"
        },
        "summary": {"total": 5, "red": 1, "orange": 1, "yellow": 2, "green": 1},
        "alerts": [
            {"comune": "Bormio", "provincia": "SO", "lat": 46.466, "lon": 10.370,
             "alert_level": "ROSSO", "risk_score": 85, "precipitation_mm": 65},
            {"comune": "Como", "provincia": "CO", "lat": 45.808, "lon": 9.085,
             "alert_level": "ARANCIONE", "risk_score": 58, "precipitation_mm": 38},
            {"comune": "Bergamo", "provincia": "BG", "lat": 45.698, "lon": 9.677,
             "alert_level": "GIALLO", "risk_score": 45, "precipitation_mm": 30},
            {"comune": "Brescia", "provincia": "BS", "lat": 45.541, "lon": 10.211,
             "alert_level": "GIALLO", "risk_score": 35, "precipitation_mm": 25},
            {"comune": "Milano", "provincia": "MI", "lat": 45.464, "lon": 9.190,
             "alert_level": "VERDE", "risk_score": 15, "precipitation_mm": 10}
        ],
        "critical_areas": []
    })


@app.route('/')
def serve_index():
    """Serve il file index.html."""
    return send_from_directory(str(FRONTEND_DIR), 'index.html')


@app.route('/<path:path>')
def serve_static(path):
    """Serve file statici dal frontend."""
    file_path = FRONTEND_DIR / path
    
    if file_path.exists():
        return send_from_directory(str(FRONTEND_DIR), path)
    
    return f"File non trovato: {path}", 404


@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "frontend_exists": FRONTEND_DIR.exists(),
        "data_dir_exists": DATA_DIR.exists(),
        "api_key_configured": bool(os.getenv('ARCGIS_API_KEY'))
    })


if __name__ == '__main__':
    logger.info(f" Frontend: {FRONTEND_DIR}")
    logger.info(f" Data: {DATA_DIR}")
    
    # Verifica file critici
    for file in ['index.html', 'script.js', 'styles.css']:
        path = FRONTEND_DIR / file
        if path.exists():
            logger.info(f" {file} trovato")
        else:
            logger.error(f" {file} MANCANTE")
    
    port = int(os.environ.get("PORT", 5001))
    logger.info(f" Server avviato su http://localhost:{port}")
    
    app.run(host='0.0.0.0', port=port, debug=True)