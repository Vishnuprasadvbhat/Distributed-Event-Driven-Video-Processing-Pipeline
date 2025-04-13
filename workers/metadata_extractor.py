import ffmpeg 
import pika
import json
import time 
import os 
import logging
time_str = time.strftime('%Y%m%d_%H%M%S')


OUTPUT_DIR = os.path.join('metadata')
os.makedirs(OUTPUT_DIR, exist_ok=True)

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.exchange_declare(exchange='video_processing', exchange_type='fanout')


channel.queue_declare(queue='metadata_extraction')
channel.queue_bind(exchange='video_processing', queue='metadata_extraction')

def save_metadata(metadata_dir, video_path, metadata):
 
    try:
      
        os.makedirs(metadata_dir, exist_ok=True)

        filename = os.path.splitext(os.path.basename(video_path))[0]
        file_path = os.path.join(metadata_dir, f"{filename}_metadata.json")

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4)
        
        logging.info(f"Metadata saved successfully: {file_path}")
        return file_path
        
    except Exception as e:
        logging.error(f"Error while saving metadata: {str(e)}")
        return None

def extract_metadata(video_path):

    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next((stream for stream in probe["streams"] if stream["codec_type"] == "video"), None)
        
        if video_stream:
            metadata = {
                "duration": float(probe["format"]["duration"]),
                "width": video_stream["width"],
                "height": video_stream["height"],
                "frame_rate": eval(video_stream["r_frame_rate"]),  
            }
        else:
            metadata = {}

        filepath = save_metadata(metadata_dir='metadata',video_path=video_path,metadata=metadata)

        print(f" [x] Metadata Extracted: {metadata} at {time_str} saved in {filepath}")


        return metadata
    
    except ffmpeg.Error as e:
        print("FFmpeg Error:", e.stderr.decode())
        return {}


def callback(ch, method, properties, body):
    
    """Processes messages from RabbitMQ"""
    message = json.loads(body)
    video_path = message['video_path']
    print(f" [x] Received {video_path} for Metadata Extraction")
    
    metadata = extract_metadata(video_path)

    # Notify FastAPI Server (TODO)
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Consume Messages
channel.basic_consume(queue='metadata_extraction', on_message_callback=callback)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


if __name__ == "__main__":
    extract_metadata()