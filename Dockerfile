ARG BASE_IMAGE=nansencenter/geospaas:latest

FROM ${BASE_IMAGE} AS base

ARG METANORM_VERSION
RUN pip install --upgrade --no-cache-dir \
    https://github.com/nansencenter/metanorm/releases/download/${METANORM_VERSION}/metanorm-${METANORM_VERSION}-py3-none-any.whl \
    'copernicusmarine' \
    'feedparser==6.0.*' \
    'graypy==2.1.*' \
    'requests_oauthlib==1.3.*' \
    'tblib'

ARG PYTHESINT_VERSION=''
RUN bash -c "[ -n '$PYTHESINT_VERSION' ] && pip install --upgrade 'pythesint==$PYTHESINT_VERSION' || true"

FROM base

COPY . /tmp/setup
RUN pip install /tmp/setup && \
    rm -rf /tmp/setup

ENTRYPOINT ["python"]
CMD ["-m", "geospaas_harvesting.cli"]
