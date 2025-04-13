import pika
import json
import time 

time_str  = time.strftime('%Y%m%d- %H%M%S')


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Declare Exchange (Fanout)
channel.exchange_declare(exchange='video_processing', exchange_type='fanout')

def publish_task(video_path):
    """Publishes a task message to RabbitMQ"""
    message = json.dumps({"video_path": video_path})
    channel.basic_publish(exchange='video_processing', routing_key='', body=message)
    print(f" [x] Sent Task for {video_path}")



