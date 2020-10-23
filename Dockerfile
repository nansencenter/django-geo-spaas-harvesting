ARG BASE_IMAGE=nansencenter/geospaas:latest-slim

FROM ${BASE_IMAGE} as base

ARG METANORM_VERSION
RUN pip install --no-cache-dir \
    https://github.com/nansencenter/metanorm/releases/download/${METANORM_VERSION}/metanorm-${METANORM_VERSION}-py3-none-any.whl \
    graypy==2.1.0 \
    feedparser==5.2.1

FROM base

WORKDIR /tmp/setup
COPY setup.py README.md ./
COPY geospaas_harvesting ./geospaas_harvesting
RUN python setup.py bdist_wheel && \
    pip install -v dist/geospaas_harvesting-*.whl && \
    cd .. && rm -rf setup/
WORKDIR /

ENTRYPOINT ["python"]
CMD ["-m", "geospaas_harvesting.harvest"]
