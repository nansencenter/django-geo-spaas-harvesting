ARG BASE_IMAGE=nansencenter/geospaas:latest

# This stage can be used to run tests
FROM ${BASE_IMAGE} AS base
COPY . /tmp/setup
RUN pip install --upgrade --no-cache-dir -r /tmp/setup/requirements.txt

# This stage contains the full installation
FROM base
RUN pip install /tmp/setup && \
    rm -rf /tmp/setup
ENTRYPOINT ["python"]
CMD ["-m", "geospaas_harvesting.cli"]
