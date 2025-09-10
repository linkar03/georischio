import os
import sys
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, send_file
from flask_cors import CORS
from dotenv import load_dotenv
import logging

# Configurazione del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

#Directory parent al path per permettere import corretti
sys.path.insert(0, str(Path(__file__).parent.parent))

# Carica le variabili d'ambiente dal file .env nella cartella backend
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Definisci i percorsi corretti per frontend e backend
BACKEND_DIR = Path(__file__).parent
PROJECT_ROOT = BACKEND_DIR.parent
FRONTEND_DIR = PROJECT_ROOT / 'frontend'
DATA_DIR = PROJECT_ROOT / 'data'
FRONTEND_DATA_DIR = FRONTEND_DIR / 'data'

# Verifica che le directory esistano
if not FRONTEND_DIR.exists():
    logger.error(f"Directory frontend non trovata: {FRONTEND_DIR}")
    
# Inizializza Flask con la directory frontend come static folder
app = Flask(__name__, 
            static_folder=str(FRONTEND_DIR),
            static_url_path='')

# Abilita CORS per permettere richieste dal browser
CORS(app)

@app.route('/api/config')
def get_api_key():
    "Endpoint sicuro per fornire la chiave API ArcGIS al frontend."
    api_key = os.getenv('ARCGIS_API_KEY')
    if not api_key:
        logger.error("ARCGIS_API_KEY non è stata trovata nel file .env!")
        return jsonify({"error": "Configurazione della chiave API mancante sul server."}), 500
    
    logger.info("Chiave API di ArcGIS fornita al client in modo sicuro.")
    return jsonify({"apiKey": api_key})

@app.route('/api/alerts')
def get_alerts_data():
    """Fornisce i dati delle allerte dal file JSON."""
    # Cerca il file alerts_data.json generato dalla pipeline in diversi percorsi possibili
    possible_paths = [
        FRONTEND_DATA_DIR / 'alerts_data.json',
        DATA_DIR / 'alerts_data.json',
        DATA_DIR / 'predictions' / 'alerts_data.json',
    ]
    
    for path in possible_paths:
        if path.exists():
            logger.info(f"Dati allerte trovati in: {path}")
            return send_file(str(path), mimetype='application/json')
    
    # Se non trova il file, ritorna dati demo
    logger.warning("File alerts_data.json non trovato, invio dati demo")
    demo_data = {
        "metadata": {
            "title": "Georisk Sentinel Lombardia - Demo",
            "timestamp": "2025-01-01T12:00:00"
        },
        "summary": {"total": 12, "red": 2, "orange": 3, "yellow": 4, "green": 3},
        "alerts": [
            {"comune": "Bormio", "provincia": "SO", "lat": 46.466, "lon": 10.370, 
             "alert_level": "ROSSO", "risk_score": 85, "precipitation_mm": 65}
        ]
    }
    return jsonify(demo_data)

@app.route('/')
def serve_index():
    "Serve il file index.html dalla cartella frontend."
    index_path = FRONTEND_DIR / 'index.html'
    if not index_path.exists():
        logger.error(f"index.html non trovato in: {index_path}")
        return f"Errore: index.html non trovato in {FRONTEND_DIR}", 404
    return send_from_directory(str(FRONTEND_DIR), 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    "Serve tutti i file statici dalla cartella frontend."
    file_path = FRONTEND_DIR / path
    if file_path.exists():
        return send_from_directory(str(FRONTEND_DIR), path)
    else:
        logger.warning(f"File non trovato: {path}")
        return f"File non trovato: {path}", 404

@app.route('/health')
def health_check():
    "Endpoint per verificare che il server sia attivo."
    return jsonify({
        "status": "healthy",
        "backend_dir": str(BACKEND_DIR),
        "frontend_dir": str(FRONTEND_DIR),
        "frontend_exists": FRONTEND_DIR.exists(),
        "env_loaded": os.getenv('ARCGIS_API_KEY') is not None
    })

if __name__ == '__main__':
    # Log per debugging
    logger.info(f"Backend directory: {BACKEND_DIR}")
    logger.info(f"Frontend directory: {FRONTEND_DIR}")
    logger.info(f"Frontend exists: {FRONTEND_DIR.exists()}")
    
    # Verifica esistenza file critici
    critical_files = [
        FRONTEND_DIR / 'index.html',
        FRONTEND_DIR / 'script.js',
        FRONTEND_DIR / 'styles.css'
    ]
    
    for file in critical_files:
        if file.exists():
            logger.info(f"✅ {file.name} trovato")
        else:
            logger.error(f"❌ {file.name} NON trovato in {file.parent}")
    
    port = int(os.environ.get("PORT", 5001))
    logger.info(f"🚀 Server in avvio su http://localhost:{port}")
    logger.info(f"📁 Serving static files from: {FRONTEND_DIR}")
    
    app.run(host='0.0.0.0', port=port, debug=True)