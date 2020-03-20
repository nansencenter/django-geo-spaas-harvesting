FROM nansencenter/geospaas:v0.4 as base

ARG METANORM_VERSION=0.0.2
RUN pip install --no-cache-dir \
    https://github.com/nansencenter/metanorm/releases/download/${METANORM_VERSION}/metanorm-${METANORM_VERSION}-py3-none-any.whl \
    graypy==2.1.0 \
    feedparser==5.2.1

WORKDIR /tmp/setup
COPY setup.py README.md ./
COPY geospaas_harvesting ./geospaas_harvesting
RUN python setup.py bdist_wheel && \
    pip install -v dist/geospaas_harvesting-*.whl && \
    cd .. && rm -rf setup/
WORKDIR /

ENTRYPOINT ["python", "-m", "geospaas_harvesting.harvest"]
