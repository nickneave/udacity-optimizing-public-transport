"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"


# KSQL statement to create a 'turnstile' table,
# and a 'turnstile summary' aggregate table from 'turnstile'

KSQL_STATEMENT = """
CREATE TABLE turnstile (

    station_id INT,
    station_name VARCHAR,
    line VARCHAR

) WITH (
    kafka_topic = 'org.chicago.cta.turnstile.v1',
    value_format = 'avro',
    key = 'station_id'

);

CREATE TABLE turnstile_summary

WITH (
    value_format = 'json'
) AS
SELECT
    station_id
  , CAST(COUNT(station_id) AS BIGINT) AS COUNT
FROM turnstile
GROUP BY
  station_id

;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
