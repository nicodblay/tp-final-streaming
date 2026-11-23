# Flink con los SDK de Beam (Java para KafkaIO y Python para nuestras transformaciones).
# Basado en el laboratorio de la Clase 7.
ARG BEAM_VERSION=2.74.0
ARG FLINK_VERSION=1.19.3
FROM apache/beam_java17_sdk:${BEAM_VERSION} AS beam-java
FROM apache/beam_python3.12_sdk:${BEAM_VERSION} AS beam-python
FROM flink:${FLINK_VERSION}-java17

USER root
COPY --from=beam-java /opt/apache/beam /opt/apache/beam
COPY --from=beam-python /usr/local /usr/local
COPY --from=beam-python /opt/apache/beam /opt/flota/python-sdk
COPY src /opt/flota/src
ENV PYTHONPATH=/opt/flota/src \
    RUN_PYTHON_SDK_IN_DEFAULT_ENVIRONMENT=1
RUN chmod 755 /opt/apache/beam/boot /opt/flota/python-sdk/boot
USER flink
