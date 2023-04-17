# Create a dockerfile based on jupyter/all-spark-notebook:2023-03-06
# to install plotly and kaleido for offline plotting

FROM jupyter/all-spark-notebook:2023-03-06
COPY --chown=${NB_UID}:${NB_GID} requirements.txt /tmp/
RUN pip install --no-cache-dir --requirement /tmp/requirements.txt && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
# Esto es una guarrada...
ENV PYTHONPATH=/usr/local/spark/python/lib/py4j-0.10.9.5-src.zip:/usr/local/spark/python:$PYTHONPATH