// Import moduli ArcGIS
import esriConfig from "@arcgis/core/config.js";
import Map from "@arcgis/core/Map.js";
import MapView from "@arcgis/core/views/MapView.js";
import GraphicsLayer from "@arcgis/core/layers/GraphicsLayer.js";
import Graphic from "@arcgis/core/Graphic.js";
import Point from "@arcgis/core/geometry/Point.js";
import SimpleMarkerSymbol from "@arcgis/core/symbols/SimpleMarkerSymbol.js";
import PopupTemplate from "@arcgis/core/PopupTemplate.js";
import Search from "@arcgis/core/widgets/Search.js";
import BasemapToggle from "@arcgis/core/widgets/BasemapToggle.js";

// Configurazione
const CONFIG = {
    api: {
        configEndpoint: '/api/config',
        alertsEndpoint: '/api/alerts'
    },
    colors: {
        ROSSO: [255, 71, 87],
        ARANCIONE: [255, 159, 67],
        GIALLO: [255, 211, 44],
        VERDE: [38, 222, 129]
    },
    map: {
        basemap: "osm",
        center: [9.8, 45.8],
        zoom: 8
    }
};

// Stato dell'applicazione
const app = {
    map: null,
    view: null,
    graphicsLayer: null,
    data: null
};

/**
 * Recupera la chiave API dal backend in modo sicuro
 */
async function getApiKey() {
    try {
        console.log('📡 Richiesta chiave API al backend...');
        const response = await fetch(CONFIG.api.configEndpoint);
        
        if (!response.ok) {
            throw new Error(`Errore dal server: ${response.status} ${response.statusText}`);
        }
        
        const config = await response.json();
        
        if (!config.apiKey) {
            throw new Error("La chiave API non è stata fornita dal backend.");
        }
        
        console.log("Chiave API recuperata con successo.");
        return config.apiKey;
        
    } catch (error) {
        console.error("Errore nel recupero della chiave API:", error);
        
        // Mostra errore all'utente
        showError("Impossibile connettersi al sistema", 
                 "La chiave API non può essere caricata. Verifica che il server sia attivo.");
        return null;
    }
}

/**
 * Carica i dati delle allerte dal backend
 */
async function loadAlertsData() {
    console.log('Caricamento dati allerte...');
    
    try {
        // Prima prova l'endpoint API
        const response = await fetch(CONFIG.api.alertsEndpoint);
        
        if (response.ok) {
            const data = await response.json();
            console.log('Dati caricati dall\'API');
            return data;
        }
    } catch (error) {
        console.warn('API non disponibile, provo percorsi locali...');
    }
    
    // Fallback: prova percorsi locali
    const fallbackPaths = [
        './data/alerts_data.json',
        '../data/alerts_data.json',
        './frontend/data/alerts_data.json'
    ];
    
    for (const path of fallbackPaths) {
        try {
            const response = await fetch(path);
            if (response.ok) {
                const data = await response.json();
                console.log(`✅ Dati caricati da ${path}`);
                return data;
            }
        } catch (e) {
            console.warn(`File non trovato: ${path}`);
        }
    }
    
    // Se tutto fallisce, usa dati demo
    console.warn('Usando dati demo di fallback');
    return getDemoData();
}

/**
 * Mostra un messaggio di errore all'utente
 */
function showError(title, message) {
    document.body.innerHTML = `
        <calcite-notice open kind="danger" icon="exclamation-triangle-f">
            <div slot="title">${title}</div>
            <div slot="message">${message}</div>
        </calcite-notice>
    `;
}

/**
 * Inizializza la mappa
 */
function initMap() {
    console.log('Inizializzazione mappa...');
    
    app.map = new Map({
        basemap: CONFIG.map.basemap
    });

    app.view = new MapView({
        container: "mappa-principale",
        map: app.map,
        center: CONFIG.map.center,
        zoom: CONFIG.map.zoom
    });

    app.graphicsLayer = new GraphicsLayer({
        title: "Allerte Rischio Idrogeologico"
    });
    app.map.add(app.graphicsLayer);

    // Aggiunta Widget
    const searchWidget = new Search({ 
        view: app.view, 
        placeholder: "Cerca località..." 
    });
    
    const basemapToggle = new BasemapToggle({ 
        view: app.view, 
        nextBasemap: "arcgis-imagery" 
    });

    app.view.ui.add(searchWidget, "top-right");
    app.view.ui.add(basemapToggle, "bottom-right");

    app.view.when(() => {
        console.log('Mappa pronta');
        main();
    });
}

/**
 * Funzione principale
 */
async function main() {
    console.log('Avvio applicazione principale...');
    
    try {
        setupEventListeners();
        await loadAndDisplayData();
        closeLoadingModal();
        
        updateConnectionStatus(true);
        
    } catch (error) {
        console.error('Errore nel caricamento:', error);
        closeLoadingModal();
        updateConnectionStatus(false);
    }
}

/**
 * Carica e visualizza i dati
 */
async function loadAndDisplayData() {
    app.data = await loadAlertsData();
    
    if (app.data && app.data.alerts) {
        displayAlertsOnMap();
        updateDashboardUI();
        console.log(`✅ ${app.data.alerts.length} allerte visualizzate`);
    } else {
        console.warn('⚠️ Nessun dato da visualizzare');
    }
}

// Visualizza le allerte sulla mappa
function displayAlertsOnMap() {
    if (!app.data?.alerts) return;

    app.graphicsLayer.removeAll();
    
    const graphics = app.data.alerts.map((alert, index) => {
        const { lon, lat, alert_level, risk_score } = alert;
        
        if (isNaN(lat) || isNaN(lon)) return null;

        const color = CONFIG.colors[alert_level] || CONFIG.colors.VERDE;
        const size = risk_score > 70 ? 16 : (risk_score > 50 ? 13 : (risk_score > 30 ? 10 : 8));

        return new Graphic({
            geometry: new Point({ longitude: lon, latitude: lat }),
            symbol: new SimpleMarkerSymbol({
                color: color,
                size: `${size}px`,
                outline: { color: [255, 255, 255], width: 1.5 }
            }),
            attributes: { ...alert, ObjectID: index },
            popupTemplate: new PopupTemplate({
                title: "{comune} ({provincia})",
                content: `
                    <b>Livello:</b> {alert_level}<br>
                    <b>Rischio:</b> {risk_score}%<br>
                    <b>Precipitazioni:</b> {precipitation_mm}mm
                `
            })
        });
    }).filter(Boolean);

    app.graphicsLayer.addMany(graphics);
}

//aggiorna l'interfaccia della dashboard
function updateDashboardUI() {
    if (!app.data) return;
    
    const { alerts = [] } = app.data;
    const criticalList = document.querySelector('#lista-aree-critiche');
    
    const criticalAlerts = alerts
        .filter(a => ['ROSSO', 'ARANCIONE'].includes(a.alert_level))
        .sort((a, b) => b.risk_score - a.risk_score)
        .slice(0, 5);

    criticalList.innerHTML = '';
    
    if (criticalAlerts.length === 0) {
        criticalList.innerHTML = `
            <calcite-notice open kind="success" icon="check-circle">
                <div slot="message">Nessuna area critica rilevata.</div>
            </calcite-notice>
        `;
    } else {
        criticalAlerts.forEach(area => {
            const item = document.createElement('calcite-list-item');
            item.label = `${area.comune} (${area.provincia})`;
            item.description = `Rischio: ${area.risk_score}%`;
            item.addEventListener('click', () => {
                app.view.goTo({ center: [area.lon, area.lat], zoom: 12 });
            });
            criticalList.appendChild(item);
        });
    }
}

//setup event listeners per i controlli UI
function setupEventListeners() {
    const filterControl = document.querySelector('#filtro-livello');
    
    if (filterControl) {
        filterControl.addEventListener('calciteSegmentedControlChange', (event) => {
            const filterValue = event.target.value;
            
            app.graphicsLayer.graphics.forEach(graphic => {
                const level = graphic.attributes.alert_level;
                
                if (filterValue === 'tutti') {
                    graphic.visible = true;
                } else if (filterValue === 'critici') {
                    graphic.visible = ['ROSSO', 'ARANCIONE'].includes(level);
                } else {
                    graphic.visible = (level === filterValue);
                }
            });
        });
    }

    const btnInfo = document.querySelector('#btn-info');
    if (btnInfo) {
        btnInfo.addEventListener('click', () => {
            alert('Georisk Sentinel Lombardia v2.1 - Sistema ML per Rischio Idrogeologico');
        });
    }
}

/* Aggiorna lo stato di connessione nel chip - Funzione disabilitata per ora
function updateConnectionStatus(isConnected) {
    const chip = document.querySelector('#stato-connessione');
    if (chip) {
        if (isConnected) {
            chip.kind = 'brand';
            chip.innerText = 'Sistema Connesso';
        } else {
            chip.kind = 'warning';
            chip.innerText = 'Modalità Offline - Dati Demo';
        }
    }
}*/

// Chiude il modal di caricamento
function closeLoadingModal() {
    const modal = document.querySelector('#modal-caricamento');
    if (modal) {
        modal.open = false;
    }
}

// Dati demo di fallback
function getDemoData() {
    return {
        metadata: {
            title: "Georisk Sentinel - Demo",
            timestamp: new Date().toISOString()
        },
        summary: {
            red: 2,
            orange: 3,
            yellow: 4,
            green: 3
        },
        alerts: [
            { comune: "Bormio", provincia: "SO", lat: 46.466, lon: 10.370, 
              alert_level: "ROSSO", risk_score: 85, precipitation_mm: 65 },
            { comune: "Livigno", provincia: "SO", lat: 46.538, lon: 10.135, 
              alert_level: "ROSSO", risk_score: 78, precipitation_mm: 58 },
            { comune: "Bergamo", provincia: "BG", lat: 45.698, lon: 9.677, 
              alert_level: "ARANCIONE", risk_score: 65, precipitation_mm: 45 },
            { comune: "Como", provincia: "CO", lat: 45.808, lon: 9.085, 
              alert_level: "ARANCIONE", risk_score: 58, precipitation_mm: 38 },
            { comune: "Milano", provincia: "MI", lat: 45.464, lon: 9.190, 
              alert_level: "VERDE", risk_score: 25, precipitation_mm: 12 }
        ]
    };
}

// Avvia l'applicazione
async function startApp() {
    console.log('🔧 Inizializzazione Georisk Sentinel...');
    
    const apiKey = await getApiKey();
    
    if (apiKey) {
        esriConfig.apiKey = apiKey;
        initMap();
    } else {
        console.warn('Avvio in modalità demo senza chiave API');
        // se non c'è l'api, inizializza comunque la mappa con funzionalita limitate
        initMap();
    }
}

startApp();