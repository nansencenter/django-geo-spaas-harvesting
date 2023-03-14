ARG BASE_IMAGE=nansencenter/geospaas:latest-slim

FROM ${BASE_IMAGE} as base

ARG METANORM_VERSION
RUN pip install --upgrade --no-cache-dir \
    https://github.com/nansencenter/metanorm/releases/download/${METANORM_VERSION}/metanorm-${METANORM_VERSION}-py3-none-any.whl \
    'feedparser==6.0.*' \
    'graypy==2.1.*' \
    'requests_oauthlib==1.3.*' \
    'tblib'

RUN python -c 'import pythesint; pythesint.update_all_vocabularies( \
{ \
    "gcmd_instrument": "9.1.5", \
    "gcmd_science_keyword": "9.1.5", \
    "gcmd_provider": "9.1.5", \
    "gcmd_platform": "9.1.5", \
    "gcmd_location": "9.1.5", \
    "gcmd_horizontalresolutionrange": "9.1.5", \
    "gcmd_verticalresolutionrange": "9.1.5", \
    "gcmd_temporalresolutionrange": "9.1.5", \
    "gcmd_project": "9.1.5", \
    "gcmd_rucontenttype": "9.1.5", \
    "mmd_access_constraints": "v3.2", \
    "mmd_activity_type": "v3.2", \
    "mmd_areas": "v3.2", \
    "mmd_operstatus": "v3.2", \
    "mmd_platform_type": "v3.2", \
    "mmd_use_constraint_type": "v3.2", \
})'

FROM base

ARG GEOSPAAS_HARVESTING_RELEASE='0.0.0dev'
WORKDIR /tmp/setup
COPY setup.py README.md ./
COPY geospaas_harvesting ./geospaas_harvesting
RUN python setup.py bdist_wheel && \
    pip install -v dist/geospaas_harvesting-*.whl && \
    cd .. && rm -rf setup/
WORKDIR /

ENTRYPOINT ["python"]
CMD ["-m", "geospaas_harvesting.harvest"]
