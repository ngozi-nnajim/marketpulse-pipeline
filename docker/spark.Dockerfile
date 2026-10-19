# ---------- base: shared by the test and runtime stages ----------
FROM apache/spark:3.5.3-python3 AS base

USER root
# psycopg: PostgreSQL driver used for idempotent upserts in foreachBatch.
RUN pip install --no-cache-dir "psycopg[binary]==3.2.*"

WORKDIR /opt/app
COPY src ./src
# Make the marketpulse package importable without installing it.
ENV PYTHONPATH=/opt/app/src

# Checkpoint folder owned by the spark user, so the mounted volume is writable.
RUN mkdir -p /opt/checkpoints && chown -R spark:spark /opt/checkpoints

# ---------- test: runs unit tests in the same environment as production ----------
FROM base AS test
RUN pip install --no-cache-dir pytest
COPY tests ./tests
# Plain `python3` (unlike spark-submit) needs pyspark and py4j added to the path.
ENV PYTHONPATH=/opt/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
USER spark
# -p no:cacheprovider: the spark user can't write pytest's cache folder here.
CMD ["python3", "-m", "pytest", "tests/processing", "-v", "-p", "no:cacheprovider"]

# ---------- runtime: the streaming job (default, because it's the last stage) ----------
FROM base AS runtime
USER spark
# --packages downloads the Kafka connector at startup. spark.jars.ivy points
# its download cache at /tmp, because the spark user has no home folder.
CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[*]", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", \
     "/opt/app/src/marketpulse/processing/stream_job.py"]
