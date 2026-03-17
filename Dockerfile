FROM python:3.12-slim

# Installa le dipendenze di sistema necessarie per Playwright e pandas
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Directory di lavoro dentro il container
WORKDIR /app

# Copia e installa le dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Installa i browser di Playwright
RUN playwright install chromium
RUN playwright install-deps chromium

# Copia il codice
COPY . .

# Comando di default
CMD ["python", "script/whoscored_downloader.py"]