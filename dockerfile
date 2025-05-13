FROM apache/airflow:2.8.1-python3.10

USER root

# Instalar o Java necessário pro PySpark funcionar
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configurar variável JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

# Copiar e instalar requisitos Python (incluindo pyspark)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

