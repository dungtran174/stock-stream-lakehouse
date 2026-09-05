FROM apache/superset:4.0.1

USER root

# Install Trino database driver for Superset
RUN pip install --no-cache-dir sqlalchemy-trino

USER superset
