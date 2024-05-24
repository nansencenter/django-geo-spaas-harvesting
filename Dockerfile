ARG BASE_IMAGE=nansencenter/geospaas:latest

FROM ${BASE_IMAGE} as base

ARG METANORM_VERSION
RUN pip install --upgrade --no-cache-dir \
    https://github.com/nansencenter/metanorm/releases/download/${METANORM_VERSION}/metanorm-${METANORM_VERSION}-py3-none-any.whl \
    'feedparser==6.0.*' \
    'graypy==2.1.*' \
    'requests_oauthlib==1.3.*' \
    'tblib'

FROM base

COPY . /tmp/setup
RUN pip install /tmp/setup && \
    rm -rf /tmp/setup

ENTRYPOINT ["python"]
CMD ["-m", "geospaas_harvesting.cli"]
