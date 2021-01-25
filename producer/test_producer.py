from dataclasses import dataclass
import time
from models import Station, Turnstile, Weather

@dataclass
class Status:
    name: str = 'test-status-name'

status = Status()

@dataclass
class Train:
    train_id: str = 'test-train-id'
    status: Status = Status()



station = Station(12345, 'pleasure house', 'yellow', direction_a='dickadise', direction_b='cuntopia')
train = Train()
# train, direction, prev_station_id, prev_direction
count = 1
while True:
    try:
        station.run(train, 'test-direction', 54321, 'test-prev-direction')
        time.sleep(2)
        print(f'Emitted {count} messages')
        count += 1
    except KeyboardInterrupt:
        break