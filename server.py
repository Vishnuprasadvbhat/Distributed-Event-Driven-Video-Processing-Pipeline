import os
import json
import asyncio
import aiofiles
import pika
from fastapi import FastAPI, WebSocket, UploadFile, File, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import time 
from queues.pika_publisher import publish_task

time_str = time.strftime('%Y%m%d_%H%M%S')

app = FastAPI()

app.mount('/static',StaticFiles(directory="static"),name='static')
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")
app.mount("/workers/enhanced_videos/", StaticFiles(directory="workers/enhanced_videos"), name="enhanced_videos")
templates = Jinja2Templates(directory='templates')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads") 
os.makedirs(UPLOAD_DIR, exist_ok=True)


video_status = {}
websockets = {}


@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await websocket.accept()
    websockets[client_id] = websocket
    print(f" [*] WebSocket Connected: {client_id}")

    try:
        while True:
            await asyncio.sleep(10)  
    except Exception as e:
        print(f" [!] WebSocket Disconnected: {client_id} - {e}")
        del websockets[client_id]

@app.get('/', response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse('index.html', {'request': request}) 

@app.post("/upload")
async def upload_video(request: Request, file: UploadFile = File(...)):
    video_path = os.path.join(UPLOAD_DIR, file.filename)
    print(video_path)

    async with aiofiles.open(video_path, 'wb') as out_file:
        content = await file.read()
        await out_file.write(content)
    
    video_status[file.filename] = {"enhanced": False, "metadata": None}

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.exchange_declare(exchange='video_processing', exchange_type='fanout')
        
        message = json.dumps({"video_path": video_path})
        channel.basic_publish(exchange='video_processing', routing_key='', body=message)
        print(f" [x] Sent Task for {video_path}")
        
        connection.close()
    except Exception as e:
        print(f" [!] Failed to publish message to RabbitMQ: {e}")
        return JSONResponse(status_code=500, content={"error": "Failed to publish task to queue"})

    applied_filters = [
    "Brightness +20%",
    "Contrast ×1.5",
    "Frame Rate: 60 fps",
    "Resolution: 1920x1080"
]
    return templates.TemplateResponse("results.html", {
        "request": request,
        "original_url": f"/uploads/{file.filename}",
        "enhanced_url": f"/workers/enhanced_videos/enhanced_{file.filename}",
        "filename": file.filename,
        "filters": applied_filters
    })


@app.post("/internal/video-enhancement-status/")
async def video_enhancement_status(data: dict):
    video_filename = os.path.basename(data["video_path"])
    video_status[video_filename]["enhanced"] = True


    await notify_client(video_filename)
    return {"status": "enhancement_done"}


@app.post("/internal/metadata-extraction-status/")
async def metadata_extraction_status(data: dict):
    video_filename = os.path.basename(data["video_path"])
    video_status[video_filename]["metadata"] = data["metadata"]

  
    await notify_client(video_filename)
    return {"status": "metadata_extracted"}

video_status = {}

@app.get("/status/{video_filename}")
async def check_video_status(video_filename: str):
    """Check the current status of a video processing pipeline"""
    status = video_status.get(video_filename, None)
    if status is None:
        return {"error": "Video not found"}
    return {"video": video_filename, "status": status}
    

async def notify_client(video_filename):
    if video_status[video_filename]["enhanced"] and video_status[video_filename]["metadata"]:
        message = json.dumps({
            "video": video_filename,
            "metadata": video_status[video_filename]["metadata"],
            "enhanced_video_url": f"/videos/{video_filename.replace('.mp4', '_enhanced.mp4')}"
        })
        for client_id, ws in websockets.items():
            await ws.send_text(message)
        print(f" [*] Sent WebSocket Update for {video_filename}")



if __name__ == "__main__":
    
    uvicorn.run(app, host='127.0.0.1', port=8080)

