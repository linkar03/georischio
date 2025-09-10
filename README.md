Georisk sentinel lombardia

POC di sistema ML per monitoraggio rischio idrogeologico.

-- Importante --
Al momento è una demo che utilizza dati sintetici generati artificialmente, non dati reali la cui implementazione è gia predisposta e prevista per il futuro

-- Approccio Architetturale: Dati Statici vs. Servizio Online --
Questo progetto adotta deliberatamente un'architettura basata sulla generazione di file di dati statici (JSON) locali, serviti tramite il backend integrato anzichè appoggiarsi a "hosted feature leayer" in arcgis online.
Questo per:

- Compatibilità con Account Gratuiti: L'obiettivo era creare un POC completo sfruttando il tier gratuito di ArcGIS for Developers. Questo approccio non consuma alcun credito ArcGIS per l'hosting dei dati o per il traffico, in quanto vengono utilizzati solo il Maps SDK for JavaScript e le basemap, entrambi gratuiti.

- Autonomia e Portabilità : La containerizzazione con Docker include sia la logica di calcolo che i dati di output. L'applicazione è autonoma ed eseguibile da qualsiasi macchina senza necessita di dipendenze esterne

- Performance e Semplicità: Visto che gli aggiornamenti in questo caso sono periodici caricare un file JSON pre-generato è una soluzione performanete e a bassa latenza per il client. L'architettura risulta più semplice e con meno punti di potenziale fallimento rispetto ad altri approcci.

-- COSA FA --

Predice il rischio di frane e allagamenti in aree sensibili in lombardia usando Machine Learning su:
Eventi storici (fittizi nel POC)
Dati terreno (DEM sintetico)
Precipitazioni (simulate)

Visualizza i risultati su mappa interattiva con punti colorati per livello di rischio.

-- STACK --

Backend: Python, Flask, XGBoost, GeoPandas
Frontend: ArcGIS JS API (free tier), Calcite Components
Deploy: Docker


-- DOCKER --
```bash
docker-compose build
docker-compose up -d
docker-compose run --rm pipeline  # genera predizioni
# Apri http://localhost:5001


### Locale
bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Crea backend/.env con ARCGIS_API_KEY=tua_chiave
python backend/pipeline.py
python backend/server.py
# Apri http://localhost:5001


## Struttura

backend/
  ├── pipeline.py       # Training ML e predizioni
  ├── server.py         # API Flask
  ├── ml_predittore.py  # Modello XGBoost
  └── dati.py          # Generazione dati sintetici

frontend/
  ├── index.html       # UI
  ├── script.js        # Logica mappa
  └── data/           # JSON predizioni

data/                 # Dati raw e processati
models/              # Modelli ML salvati
```

-- TODO --

-Dati reali da ISPRA/ARPA
-GitHub Actions per aggiornamento orario una volta ottenuto flusso di dati reali
-Validazione con eventi storici
-Heatmap e visualizzazioni 3D 
-Miglioramenti best practices di sicurezza