FROM python:3.9-slim

# Imposta la directory di lavoro
WORKDIR /app

# IMPOrtante , installa dipendenze di sistema per librerie geospaziali
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Imposta variabili d'ambiente
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY requirements.txt .

# Installazione dipendenze Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia tutto il codice dell'applicazione
COPY . .

# Crea le directory necessarie
RUN mkdir -p data/raw data/processed data/predictions \
             models logs reports frontend/data

# Esponi la porta del server Flask
EXPOSE 5001

# Comando di default per avviare il server
CMD ["python", "backend/server.py"]