import esriConfig from "@arcgis/core/config.js";
import Map from "@arcgis/core/Map.js";
import MapView from "@arcgis/core/views/MapView.js";
import GraphicsLayer from "@arcgis/core/layers/GraphicsLayer.js";
import Graphic from "@arcgis/core/Graphic.js";
import PopupTemplate from "@arcgis/core/PopupTemplate.js";
import Search from "@arcgis/core/widgets/Search.js";
import BasemapToggle from "@arcgis/core/widgets/BasemapToggle.js";

// --- Costanti e Configurazione ---

const CONSTANTS = {
    API: {
        CONFIG: '/api/config',
        ALERTS: '/api/alerts'
    },
    ALERT_LEVELS: {
        RED: 'ROSSO',
        ORANGE: 'ARANCIONE',
        YELLOW: 'GIALLO',
        GREEN: 'VERDE'
    },
    FILTERS: {
        ALL: 'tutti',
        CRITICAL: 'critici'
    },
    COLORS: {
        ROSSO: [255, 71, 87],
        ARANCIONE: [255, 159, 67],
        GIALLO: [255, 211, 44],
        VERDE: [38, 222, 129]
    }
};

// --- Stato dell'Applicazione ---
const app = {
    map: null,
    view: null,
    graphicsLayer: null,
    alertsData: []
};

// --- Funzioni Helper ---


const isCritical = (level) => [CONSTANTS.ALERT_LEVELS.RED, CONSTANTS.ALERT_LEVELS.ORANGE].includes(level);


function getSymbolSize(riskScore) {
    if (riskScore > 70) return 16;
    if (riskScore > 50) return 13;
    if (riskScore > 30) return 10;
    return 8;
}



//Inizializza e avvia l'applicazione.
async function initialize() {
    document.addEventListener('calcitePanelScroll', event => event.stopPropagation());
    const apiKey = await getApiKey();
    if (apiKey) {
        esriConfig.apiKey = apiKey;
    }
    initMap();
    initUI();
    await loadAndDisplayData();
    closeLoadingModal();
}


//Inizializza la mappa e i suoi widget.
function initMap() {
    app.map = new Map({ basemap: "arcgis-topographic" });
    app.view = new MapView({
        container: "mappa-principale",
        map: app.map,
        center: [9.8, 45.8],
        zoom: 8
    });

    app.graphicsLayer = new GraphicsLayer({ title: "Allerte Rischio Idrogeologico" });
    app.map.add(app.graphicsLayer);

    const search = new Search({ view: app.view, placeholder: "Cerca località..." });
    const basemapToggle = new BasemapToggle({ view: app.view, nextBasemap: "arcgis-imagery" });
    app.view.ui.add(search, "top-right");
    app.view.ui.add(basemapToggle, "bottom-right");
}


//Imposta i gestori di eventi per l'interfaccia utente.

function initUI() {
    document.querySelector('#filtro-livello')?.addEventListener('calciteSegmentedControlChange', (event) => {
        filterMapGraphics(event.target.value);
    });

    document.querySelector('#btn-info')?.addEventListener('click', () => {
        alert('Georisk Sentinel Lombardia v2.1 - Sistema ML per Rischio Idrogeologico');
    });
}

// Carica i dati delle allerte e aggiorna la UI.
async function loadAndDisplayData() {
    const data = await fetchAlerts();
    if (data && data.alerts) {
        app.alertsData = data.alerts;
        displayAlertsOnMap(app.alertsData);
        updateDashboard(app.alertsData);
    } else {
        console.warn('Nessun dato di allerta da visualizzare.');
        updateDashboard([]);
    }
}

// --- Funzioni di Interazione con l'API ---

async function getApiKey() {
    try {
        const response = await fetch(CONSTANTS.API.CONFIG);
        if (!response.ok) throw new Error(`Errore server: ${response.status}`);
        const config = await response.json();
        return config.apiKey;
    } catch (error) {
        showError("Impossibile caricare la configurazione dell'applicazione.");
        console.error("Errore nel recupero della chiave API:", error);
        return null;
    }
}

async function fetchAlerts() {
    try {
        const response = await fetch(CONSTANTS.API.ALERTS);
        if (!response.ok) throw new Error('API allerte non raggiungibile');
        return await response.json();
    } catch (error) {
        console.warn(`${error.message}, uso dati di fallback.`);
        return getDemoData();
    }
}

// --- Funzioni di Aggiornamento UI ---

function displayAlertsOnMap(alerts) {
    app.graphicsLayer.removeAll();
    const graphics = alerts.map(alert => {
        const { lon, lat, alert_level, risk_score } = alert;
        if (isNaN(lat) || isNaN(lon)) return null;

        const graphic = new Graphic({
            geometry: { type: "point", longitude: lon, latitude: lat },
            symbol: {
                type: "simple-marker",
                color: CONSTANTS.COLORS[alert_level] || CONSTANTS.COLORS.VERDE,
                size: `${getSymbolSize(risk_score)}px`,
                outline: { color: "white", width: 1.5 }
            },
            attributes: alert,
            popupTemplate: new PopupTemplate({
                title: "{comune} ({provincia})",
                content: `<b>Livello:</b> {alert_level}<br><b>Rischio:</b> {risk_score}%`
            })
        });
        return graphic;
    }).filter(Boolean); // Rimuove eventuali null (coordinate non valide)

    app.graphicsLayer.addMany(graphics);
}

function updateDashboard(alerts) {
    const criticalList = document.querySelector('#lista-aree-critiche');
    criticalList.innerHTML = '';

    const criticalAlerts = alerts
        .filter(a => isCritical(a.alert_level))
        .sort((a, b) => b.risk_score - a.risk_score)
        .slice(0, 5);

    if (criticalAlerts.length === 0) {
        const notice = document.createElement('calcite-notice');
        notice.open = true;
        notice.kind = 'success';
        notice.icon = 'check-circle';
        notice.innerHTML = `<div slot="message">Nessuna area critica rilevata.</div>`;
        criticalList.appendChild(notice);
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

function filterMapGraphics(filterValue) {
    app.graphicsLayer.graphics.forEach(graphic => {
        const level = graphic.attributes.alert_level;
        if (filterValue === CONSTANTS.FILTERS.ALL) {
            graphic.visible = true;
        } else if (filterValue === CONSTANTS.FILTERS.CRITICAL) {
            graphic.visible = isCritical(level);
        } else {
            graphic.visible = (level === filterValue);
        }
    });
}

function closeLoadingModal() {
    const modal = document.querySelector('#modal-caricamento');
    if (modal) modal.open = false;
}

function showError(message) {
    const modal = document.querySelector('#modal-caricamento');
    if (modal) {
        modal.querySelector('[slot="header"]').textContent = 'Errore';
        modal.querySelector('[slot="content"]').innerHTML = `
            <p style="padding: 1rem;">${message}</p>
        `;
    }
}

// --- Dati di Fallback ---
function getDemoData() {
    return {
        alerts: [
            { comune: "Bormio", provincia: "SO", lat: 46.466, lon: 10.370, alert_level: "ROSSO", risk_score: 85 },
            { comune: "Livigno", provincia: "SO", lat: 46.538, lon: 10.135, alert_level: "ROSSO", risk_score: 78 },
            { comune: "Como", provincia: "CO", lat: 45.808, lon: 9.085, alert_level: "ARANCIONE", risk_score: 58 },
            { comune: "Milano", provincia: "MI", lat: 45.464, lon: 9.190, alert_level: "VERDE", risk_score: 25 }
        ]
    };
}

initialize();