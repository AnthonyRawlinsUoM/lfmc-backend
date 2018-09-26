# lfmc-api

## README

Intended as a component to the LFMC Server Ecosystem. This is an image that answers Temporal Queries to form Time-series data in JSON for analysis of Landscape Fuel Moisture Conditions.

Para also collects input data from a variety of sources and starts ingestion task on the GeoMesa Cluster.

Processes, Models and TS Data are all exposed through as RESTful API.

Hug is used for API development and Hug also naturally exposes a WSGI-Compatible API ready for production use.

### Building the Docker Image
    $ docker build . --tag anthonyrawlinsuom/lfmc-api

### Running the Docker Image
    $ docker run -it -p 8002:8002 anthonyrawlinsuom/lfmc-api /bin/bash -exec 'hug -f LFMCServer.py'
