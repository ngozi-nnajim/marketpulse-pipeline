# Spark streaming job image, based on the official Apache Spark image.
FROM apache/spark:3.5.3-python3

USER root
# psycopg: PostgreSQL driver used for idempotent upserts in foreachBatch.
RUN pip install --no-cache-dir "psycopg[binary]==3.2.*"

WORKDIR /opt/app
COPY src ./src
# Make the marketpulse package importable without installing it.
ENV PYTHONPATH=/opt/app/src

# Create the checkpoint folder owned by the spark user, so the named volume
# mounted here inherits writable permissions.
RUN mkdir -p /opt/checkpoints && chown -R spark:spark /opt/checkpoints

# Run as the image's non-root user.
USER spark

# --packages downloads the Kafka connector at startup. spark.jars.ivy points
# its download cache at /tmp, because the spark user has no home folder.
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
     "/opt/app/src/marketpulse/processing/stream_job.py"]
