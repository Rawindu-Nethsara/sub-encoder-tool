from fastapi import FastAPI, APIRouter, File, UploadFile, Form, BackgroundTasks, HTTPException, Depends
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone
import asyncio
import aiofiles
import aiohttp
import subprocess
import json
import time
import hashlib
import tempfile
import magic
import shutil
from urllib.parse import urlparse
import requests

# Load environment variables
from dotenv import load_dotenv
ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

app = FastAPI(title="Video Subtitle Hard-Encoder & Archive.org Uploader", version="1.0.0")
api_router = APIRouter(prefix="/api")

# Archive.org credentials
ARCHIVE_ACCESS_KEY = "3uJG4iX4yWLXbqO5"
ARCHIVE_SECRET_KEY = "QFrPpeHItENOhpil"
ARCHIVE_IDENTIFIER = "moovieds-official-uploader"

# Models
class ProcessingJob(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: str = "queued"  # queued, processing, completed, failed
    video_filename: str
    subtitle_filename: str
    progress: float = 0.0
    encoding_time: float = 0.0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    archive_url: Optional[str] = None
    video_size: Optional[int] = None
    processed_size: Optional[int] = None

class ProcessingStatus(BaseModel):
    job_id: str
    status: str
    progress: float
    encoding_time: float
    message: str
    archive_url: Optional[str] = None

# Global variables for tracking jobs
active_jobs = {}
upload_progress = {}

# Video processing service
class VideoProcessor:
    def __init__(self):
        self.temp_dir = Path("/tmp/video_processing")
        self.temp_dir.mkdir(exist_ok=True)
    
    async def validate_video_file(self, file_path: Path) -> Dict[str, Any]:
        """Validate video file format and properties"""
        if not file_path.exists():
            raise HTTPException(status_code=400, detail="Video file not found")
        
        try:
            # Check file size (max 3GB)
            file_size = file_path.stat().st_size
            if file_size > 3 * 1024 * 1024 * 1024:  # 3GB
                raise HTTPException(status_code=400, detail="Video file exceeds 3GB limit")
            
            # Check MIME type
            mime_type = magic.from_file(str(file_path), mime=True)
            if not mime_type.startswith('video/'):
                raise HTTPException(status_code=400, detail=f"Invalid file type: {mime_type}")
            
            # Get video info using ffprobe
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise HTTPException(status_code=400, detail="Invalid video file format")
            
            info = json.loads(result.stdout)
            duration = float(info['format']['duration'])
            
            return {
                'size': file_size,
                'duration': duration,
                'mime_type': mime_type,
                'valid': True
            }
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Video validation failed: {str(e)}")
    
    async def validate_subtitle_file(self, file_path: Path) -> bool:
        """Validate subtitle file format"""
        if not file_path.exists():
            raise HTTPException(status_code=400, detail="Subtitle file not found")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read(1000)  # Read first 1000 characters
            
            # Check if it's ASS format
            if '[Script Info]' in content or '[V4+ Styles]' in content:
                return True
            
            # Check if it's SRT format (we'll convert to ASS)
            if '-->' in content and any(c.isdigit() for c in content[:100]):
                return True
            
            raise HTTPException(status_code=400, detail="Unsupported subtitle format. Please use SRT or ASS files.")
            
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Subtitle validation failed: {str(e)}")
    
    async def convert_srt_to_ass(self, srt_path: Path, ass_path: Path) -> None:
        """Convert SRT subtitle to ASS format"""
        cmd = [
            'ffmpeg', '-i', str(srt_path), '-y', str(ass_path)
        ]
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"SRT to ASS conversion failed: {stderr.decode()}")
    
    async def encode_subtitles(
        self, 
        video_path: Path, 
        subtitle_path: Path, 
        output_path: Path,
        job_id: str
    ) -> Dict[str, Any]:
        """Hard-encode subtitles into video"""
        
        start_time = time.time()
        
        try:
            # Prepare subtitle file
            working_subtitle_path = subtitle_path
            
            # Convert SRT to ASS if needed
            if subtitle_path.suffix.lower() == '.srt':
                ass_path = self.temp_dir / f"{job_id}_converted.ass"
                await self.convert_srt_to_ass(subtitle_path, ass_path)
                working_subtitle_path = ass_path
            
            # Build FFmpeg command for hard-encoding subtitles
            cmd = [
                'ffmpeg',
                '-i', str(video_path),
                '-vf', f'ass={working_subtitle_path}',
                '-c:v', 'libx264',
                '-c:a', 'aac',
                '-preset', 'medium',
                '-crf', '23',
                '-movflags', '+faststart',
                '-y',
                str(output_path)
            ]
            
            # Start FFmpeg process
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT
            )
            
            # Monitor progress
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                
                line = line.decode('utf-8', errors='ignore').strip()
                
                # Parse progress from FFmpeg output
                if 'time=' in line:
                    # Extract time information for progress calculation
                    import re
                    time_match = re.search(r'time=(\d{2}):(\d{2}):(\d{2}\.\d{2})', line)
                    if time_match:
                        hours, minutes, seconds = time_match.groups()
                        current_time = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
                        
                        # Update progress in database
                        job = active_jobs.get(job_id)
                        if job and 'duration' in job:
                            progress = min((current_time / job['duration']) * 100, 100)
                            await self.update_job_progress(job_id, progress, time.time() - start_time)
            
            # Wait for process completion
            await process.wait()
            
            if process.returncode != 0:
                raise Exception("FFmpeg encoding failed")
            
            encoding_time = time.time() - start_time
            
            return {
                'success': True,
                'encoding_time': encoding_time,
                'output_path': str(output_path),
                'size': output_path.stat().st_size
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'encoding_time': time.time() - start_time
            }
    
    async def update_job_progress(self, job_id: str, progress: float, encoding_time: float):
        """Update job progress in database"""
        try:
            await db.processing_jobs.update_one(
                {"id": job_id},
                {
                    "$set": {
                        "progress": progress,
                        "encoding_time": encoding_time,
                        "status": "processing"
                    }
                }
            )
            
            # Update in-memory tracking
            if job_id in active_jobs:
                active_jobs[job_id]['progress'] = progress
                active_jobs[job_id]['encoding_time'] = encoding_time
                
        except Exception as e:
            logging.error(f"Failed to update job progress: {e}")

# Archive.org upload service
class ArchiveUploader:
    def __init__(self):
        self.access_key = ARCHIVE_ACCESS_KEY
        self.secret_key = ARCHIVE_SECRET_KEY
        self.identifier = ARCHIVE_IDENTIFIER
        self.base_url = "https://s3.us.archive.org"
    
    async def upload_video(self, video_path: Path, job_id: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Upload processed video to Archive.org"""
        
        try:
            # Prepare metadata for Archive.org
            archive_metadata = {
                'collection': 'opensource_movies',
                'mediatype': 'movies',
                'title': metadata.get('title', f'Video {job_id}'),
                'description': metadata.get('description', 'Video with hard-encoded subtitles'),
                'creator': 'moovieds-official-uploader',
                'date': datetime.now(timezone.utc).isoformat(),
                'subject': 'video;subtitles;processed',
                'licenseurl': 'http://creativecommons.org/licenses/by/4.0/',
                'job_id': job_id
            }
            
            # Use internetarchive library for upload
            import internetarchive as ia
            
            # Configure session
            session = ia.get_session(
                config={
                    's3': {
                        'access': self.access_key,
                        'secret': self.secret_key
                    }
                }
            )
            
            # Get item
            item = session.get_item(self.identifier)
            
            # Upload file
            filename = f"{job_id}_{video_path.name}"
            response = item.upload(
                files={filename: str(video_path)},
                metadata=archive_metadata,
                verbose=True,
                verify=True
            )
            
            if response and response[0].status_code in [200, 201]:
                archive_url = f"https://archive.org/details/{self.identifier}/{filename}"
                return {
                    'success': True,
                    'archive_url': archive_url,
                    'filename': filename
                }
            else:
                return {
                    'success': False,
                    'error': f"Upload failed with status: {response[0].status_code if response else 'Unknown'}"
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

# Initialize services
video_processor = VideoProcessor()
archive_uploader = ArchiveUploader()

# Utility functions
async def save_uploaded_file(file: UploadFile, job_id: str) -> Path:
    """Save uploaded file to temporary directory"""
    file_path = video_processor.temp_dir / f"{job_id}_{file.filename}"
    
    async with aiofiles.open(file_path, 'wb') as f:
        content = await file.read()
        await f.write(content)
    
    return file_path

async def download_video_from_url(url: str, job_id: str) -> Path:
    """Download video from URL"""
    try:
        parsed_url = urlparse(url)
        filename = Path(parsed_url.path).name or f"{job_id}_video.mp4"
        file_path = video_processor.temp_dir / f"{job_id}_{filename}"
        
        # Download file
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise HTTPException(status_code=400, detail=f"Failed to download video: HTTP {response.status}")
                
                async with aiofiles.open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
        
        return file_path
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to download video: {str(e)}")

# Background processing function
async def process_video_job(job_id: str):
    """Background task to process video with subtitles"""
    
    try:
        # Get job from database
        job_doc = await db.processing_jobs.find_one({"id": job_id})
        if not job_doc:
            return
        
        job = ProcessingJob(**job_doc)
        active_jobs[job_id] = job_doc
        
        # Update status to processing
        await db.processing_jobs.update_one(
            {"id": job_id},
            {"$set": {"status": "processing"}}
        )
        
        # Get file paths
        video_path = Path(job_doc['video_path'])
        subtitle_path = Path(job_doc['subtitle_path'])
        output_path = video_processor.temp_dir / f"{job_id}_processed.mp4"
        
        # Validate files
        video_info = await video_processor.validate_video_file(video_path)
        await video_processor.validate_subtitle_file(subtitle_path)
        
        # Store duration for progress calculation
        active_jobs[job_id]['duration'] = video_info['duration']
        
        # Encode subtitles
        encoding_result = await video_processor.encode_subtitles(
            video_path, subtitle_path, output_path, job_id
        )
        
        if not encoding_result['success']:
            raise Exception(encoding_result['error'])
        
        # Upload to Archive.org
        upload_result = await archive_uploader.upload_video(
            output_path, job_id, {
                'title': f"Processed Video {job_id}",
                'description': f"Video with hard-encoded subtitles - Original: {job.video_filename}"
            }
        )
        
        if not upload_result['success']:
            raise Exception(upload_result['error'])
        
        # Update job as completed
        await db.processing_jobs.update_one(
            {"id": job_id},
            {
                "$set": {
                    "status": "completed",
                    "progress": 100.0,
                    "encoding_time": encoding_result['encoding_time'],
                    "completed_at": datetime.now(timezone.utc),
                    "archive_url": upload_result['archive_url'],
                    "processed_size": encoding_result.get('size', 0)
                }
            }
        )
        
        # Cleanup temporary files
        for temp_file in [video_path, subtitle_path, output_path]:
            if temp_file.exists():
                temp_file.unlink()
        
        # Remove from active jobs
        if job_id in active_jobs:
            del active_jobs[job_id]
            
    except Exception as e:
        # Update job as failed
        await db.processing_jobs.update_one(
            {"id": job_id},
            {
                "$set": {
                    "status": "failed",
                    "error_message": str(e),
                    "completed_at": datetime.now(timezone.utc)
                }
            }
        )
        
        # Remove from active jobs
        if job_id in active_jobs:
            del active_jobs[job_id]
        
        logging.error(f"Job {job_id} failed: {e}")

# API Endpoints
@api_router.post("/upload-files", response_model=ProcessingStatus)
async def upload_files(
    background_tasks: BackgroundTasks,
    video_file: UploadFile = File(...),
    subtitle_file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    description: Optional[str] = Form(None)
):
    """Upload video and subtitle files for processing"""
    
    job_id = str(uuid.uuid4())
    
    try:
        # Save uploaded files
        video_path = await save_uploaded_file(video_file, job_id)
        subtitle_path = await save_uploaded_file(subtitle_file, job_id)
        
        # Create job record
        job = ProcessingJob(
            id=job_id,
            video_filename=video_file.filename,
            subtitle_filename=subtitle_file.filename,
            video_size=video_path.stat().st_size
        )
        
        # Store additional paths for processing
        job_dict = job.dict()
        job_dict['video_path'] = str(video_path)
        job_dict['subtitle_path'] = str(subtitle_path)
        job_dict['title'] = title
        job_dict['description'] = description
        
        await db.processing_jobs.insert_one(job_dict)
        
        # Start background processing
        background_tasks.add_task(process_video_job, job_id)
        
        return ProcessingStatus(
            job_id=job_id,
            status="queued",
            progress=0.0,
            encoding_time=0.0,
            message="Files uploaded successfully. Processing will begin shortly."
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@api_router.post("/upload-url", response_model=ProcessingStatus)
async def upload_from_url(
    background_tasks: BackgroundTasks,
    video_url: str = Form(...),
    subtitle_file: UploadFile = File(...),
    title: Optional[str] = Form(None),
    description: Optional[str] = Form(None)
):
    """Upload video from URL and subtitle file for processing"""
    
    job_id = str(uuid.uuid4())
    
    try:
        # Download video from URL
        video_path = await download_video_from_url(video_url, job_id)
        
        # Save subtitle file
        subtitle_path = await save_uploaded_file(subtitle_file, job_id)
        
        # Create job record
        job = ProcessingJob(
            id=job_id,
            video_filename=Path(video_url).name or "video_from_url.mp4",
            subtitle_filename=subtitle_file.filename,
            video_size=video_path.stat().st_size
        )
        
        # Store additional paths for processing
        job_dict = job.dict()
        job_dict['video_path'] = str(video_path)
        job_dict['subtitle_path'] = str(subtitle_path)
        job_dict['title'] = title
        job_dict['description'] = description
        job_dict['video_url'] = video_url
        
        await db.processing_jobs.insert_one(job_dict)
        
        # Start background processing
        background_tasks.add_task(process_video_job, job_id)
        
        return ProcessingStatus(
            job_id=job_id,
            status="queued",
            progress=0.0,
            encoding_time=0.0,
            message="Video downloaded and queued for processing."
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@api_router.get("/status/{job_id}", response_model=ProcessingStatus)
async def get_job_status(job_id: str):
    """Get processing job status"""
    
    job_doc = await db.processing_jobs.find_one({"id": job_id})
    if not job_doc:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = ProcessingJob(**job_doc)
    
    return ProcessingStatus(
        job_id=job.id,
        status=job.status,
        progress=job.progress,
        encoding_time=job.encoding_time,
        message=f"Job {job.status}" + (f": {job.error_message}" if job.error_message else ""),
        archive_url=job.archive_url
    )

@api_router.get("/jobs", response_model=List[ProcessingJob])
async def get_all_jobs():
    """Get all processing jobs"""
    
    jobs = await db.processing_jobs.find().sort("created_at", -1).limit(50).to_list(50)
    return [ProcessingJob(**job) for job in jobs]

@api_router.delete("/job/{job_id}")
async def delete_job(job_id: str):
    """Delete a processing job"""
    
    result = await db.processing_jobs.delete_one({"id": job_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {"message": "Job deleted successfully"}

# Root endpoint
@api_router.get("/")
async def root():
    return {"message": "Video Subtitle Hard-Encoder & Archive.org Uploader API"}

# Include router
app.include_router(api_router)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()

# Install internetarchive library if not present
try:
    import internetarchive
except ImportError:
    logging.warning("internetarchive library not found. Please install it: pip install internetarchive")