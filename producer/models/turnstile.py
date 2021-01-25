"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

import asyncio

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(
        "/home/pczhang/Nutstore/Udacity/DataStreamingNanoDegree/project1/passed/producer/models/schemas/turnstile_key.json")
    value_schema = avro.load(
        "/home/pczhang/Nutstore/Udacity/DataStreamingNanoDegree/project1/passed/producer/models/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        super().__init__(
            'chicago.cta.station.turnstile' , # TODO: Come up with a better topic name
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=5,
            num_replicas=1,
        )
        
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)


    def run(self, timestamp, time_step):
        asyncio.run(self.run_produce(timestamp, time_step))

    
    async def run_produce(self, timestamp, time_step):
        res = asyncio.create_task(self.produce_foo(timestamp, time_step))
        await res

    async def produce_foo(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
        logger.info(f'{num_entries} have entered station {self.station.name} at {timestamp.isoformat()}')
        
        for _ in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={'timestamp': self.time_millis()},
                    value={
                        'station_id': 12345, # self.station.station_id,
                        'station_name': 'test_station_name_in_turnstile', # self.station.name,
                        'line': 'test_line_color_in_turnstile'# self.station.color.name,
                    }
                )
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.fatal(e)
                raise e

