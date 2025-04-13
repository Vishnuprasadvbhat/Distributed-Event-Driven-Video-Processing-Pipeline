import pika
import json
import ffmpeg 
import os 
import logging
import time 

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Generate timestamp format for filenames
time_str = time.strftime('%Y%m%d_%H%M%S')

# Output directory setup
OUTPUT_DIR = os.path.join("enhanced_videos")  
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_video_resolution(video_path):
    if not os.path.exists(video_path):
        logging.info('Path Error: File not Found')
        return None, None

    try:
        probe = ffmpeg.probe(video_path)
        video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)

        if video_stream:
            width = video_stream.get('width')
            height = video_stream.get('height')
            return width, height if width and height else (None, None)

        logging.info('Video Stream not found')
        return None, None

    except ffmpeg.Error:
        logging.info('Error: Probe')
        return None, None

    except Exception:
        logging.info('Error: Exception')
        return None, None

import os
import logging
import ffmpeg

def enhance_video(video_path, output_dir):
    if not os.path.exists(video_path):
        logging.info('Path Error: File not Found')
        return None

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename = os.path.basename(video_path)
    output_path = os.path.join(output_dir, f"enhanced_{filename}")

    # Ensure .mp4 extension
    if not output_path.endswith('.mp4'):
        output_path += '.mp4'

    try:
    
        stream = (
            ffmpeg.input(video_path)
            .filter("eq", brightness=0.2, contrast=1.5)
            .filter("fps", fps=60)
            .filter("scale", 1920, 1080)
        )

        
        stream = ffmpeg.output(
            stream,
            output_path,
            vcodec='libx264',
            preset='fast',
            crf=23,
            acodec='aac',
            movflags='+faststart'
        )

        ffmpeg.run(stream, overwrite_output=True, capture_stdout=True, capture_stderr=True)
        return output_path

    except ffmpeg.Error as e:
        stderr = e.stderr.decode() if hasattr(e, 'stderr') and e.stderr else "Unknown error"
        logging.error(f'FFmpeg Error: {stderr}')
        return None

    
def callback(ch, method, properties, body):
    message = json.loads(body)
    video_path = message['video_path']
    print(f" [x] Received {video_path} for Enhancement at {time.strftime('%H:%M:%S')}")
    
    enhanced_video_path = enhance_video(video_path, output_dir=OUTPUT_DIR)
    print(f" [x] Final output {enhanced_video_path} with applied enhancement at {time.strftime('%H:%M:%S')}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def start_consumer():
    try:
        # RabbitMQ Connection
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()

        # Declare Exchange
        channel.exchange_declare(exchange='video_processing', exchange_type='fanout')

        # Declare Queue
        channel.queue_declare(queue='video_enhancement')
        channel.queue_bind(exchange='video_processing', queue='video_enhancement')

        # Consume Messages
        channel.basic_consume(queue='video_enhancement', on_message_callback=callback)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Error in RabbitMQ consumer: {str(e)}")
        if 'connection' in locals() and connection.is_open:
            connection.close()

if __name__ == "__main__":
    start_consumer()