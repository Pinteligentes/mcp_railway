FROM python:3.12-slim

# Evita buffering y configura el directorio de trabajo
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# Instala dependencias del sistema si tu script las requiere
# RUN apt-get update && apt-get install -y build-essential ... && rm -rf /var/lib/apt/lists/*

# Copia código
COPY app/ ./app/

# Instala Python deps
RUN pip install --no-cache-dir -r app/requirements.txt

# Crea un directorio de datos (opcional) para salidas
RUN mkdir -p /data

# Exponer puerto (Railway lo detecta por $PORT)
EXPOSE 8080

# Comando de arranque (uvicorn ASGI)
CMD ["uvicorn", "app.mcp_http:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers"]
