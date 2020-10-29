#!/bin/bash

docker run -d --rm \
--network testing \
--name "$GEOSPAAS_DB_HOST" \
-e "POSTGRES_USER=$GEOSPAAS_DB_USER" \
-e "POSTGRES_PASSWORD=$GEOSPAAS_DB_PASSWORD" \
'postgis/postgis:12-3.0'

i=0
while ! docker exec db pg_isready && (( i < 10 ));do
    (( i += 1))
    echo "Waiting for the database"
    sleep 1
done

if (( i == 10));then
    echo "The database did not start in time"
    exit 1
else
    # The database still needs a bit of time after pg_isready indicates it is ready
    sleep 1
fi