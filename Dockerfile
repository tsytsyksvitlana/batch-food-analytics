FROM apache/spark:3.5.1

WORKDIR /app

USER root

COPY . .

RUN mkdir -p /app/data/raw /app/data/processed /app/logs \
    && chown -R spark:spark /app

USER spark

ENV PATH="/opt/spark/bin:${PATH}"
