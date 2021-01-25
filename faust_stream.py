"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream-v13", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("chicago.cta.stations", value_type=Station, )
# TODO: Define the output Kafka Topic
out_topic = app.topic("chicago.cta.stations.table", partitions=1)
# TODO: Define a Faust Table
table = app.Table("chicago.cta.stations.table", default=TransformedStation, partitions=1, changelog_topic=out_topic, )


@app.agent(topic)
async def process(stations):
    i = 0
    async for station in stations:
        i += 1

        line = None
        # print('red',station.red)
        # print('blue',station.blue)
        # print('green', station.green)

        if station.red == True:
            line = "red"
            print(f'fanculo!!!!!! {i}')
        elif station.blue == True:
            line = "blue"
            print(f'fanculo!!!!!! {i}')
        elif station.green == True:
            line = "green"
            print(f'fanculo!!!!!! {i}')

        else:
            logger.info('fucking!!! %s', station.red)
            logger.info('fucking!!! %s', station.blue)
            logger.info('fucking!!! %s', station.green)
            logger.warning('failed to find line data in %s', station)

        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )


if __name__ == "__main__":
    app.main()
