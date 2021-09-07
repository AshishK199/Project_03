import websocket
import json
import time
from kafka import KafkaProducer


def on_message(ws, message):
     producer.send('sample', message)
     producer.flush()

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),
                         bootstrap_servers=['localhost:9092'])

websocket.enableTrace(True)

ws = websocket.WebSocketApp("wss://stream.meetup.com/2/rsvps", on_message=on_message)

while(1):
    ws.run_forever()
    time.sleep(2)
