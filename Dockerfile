# Imagen base Python slim (ligera, sin extras innecesarios)
FROM python:3.12-slim

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH="$JAVA_HOME/bin:$PATH"

# Instalar Java 21 (compatible con PySpark 3.5.x)
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Copiar e instalar dependencias primero (aprovecha caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente y los datos Gold
COPY src/ ./src/
COPY data/aggregated/ ./data/aggregated/

# Puerto que expone el dashboard
EXPOSE 8050

# Comando de arranque
CMD ["python", "src/dashboard/app.py"]