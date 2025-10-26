
from datetime import datetime
from enum import Enum, StrEnum
from pathlib import Path, PosixPath
from sre_constants import SUCCESS
from xml.dom.pulldom import START_DOCUMENT
from pydantic import BaseModel, ConfigDict, AwareDatetime, Field
from uuid import UUID
from typing import List, Optional



class CustomBaseModel(BaseModel):
    """Base model for database models."""
    model_config = ConfigDict(
        from_attributes=True,
        json_encoders={
            UUID: lambda v: str(v),
            Path: lambda v: str(v),
            PosixPath: lambda v: str(v),
            datetime: lambda v: v.isoformat(),
        },
    )

class ManifestEntry(CustomBaseModel):
    """Database model for a single entry in the manifest."""
    id: UUID
    ops_key: str
    bucket_key: str


class Manifest(CustomBaseModel):
    id: UUID
    load_id: str
    manifest_file: Path
    ops_root_dir: Path
    s3_bucket_name: str
    data_folders: List[str] = []
    total_size: str = "0B"
    total_files: int = 0
    created_at: Optional[AwareDatetime] = None
    entries: Optional[List[ManifestEntry]] = None


class JobStatus(StrEnum, Enum):
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    ERROR = 'ERROR'

    class Config:  
        use_enum_values = True 
        
class Job(CustomBaseModel):
    id: UUID
    manifest_id: UUID 
    status: JobStatus = JobStatus.PENDING
    created_at: Optional[AwareDatetime] = None
    updated_at: Optional[AwareDatetime] = None
    started_at: Optional[AwareDatetime] = None
    completed_at: Optional[AwareDatetime] = None
    elapsed_time: Optional[str] = None
    uploaded_files: int = 0
    uploaded_size: Optional[str] = None
    mock: bool = False
    count: Optional[int] = None
    aws_unsigned: Optional[bool] = None

    manifest: Optional[Manifest] = None 

class JobUpdate(CustomBaseModel):
    """Model for updating a job."""
    status: Optional[JobStatus] = None
    uploaded_files: Optional[int] = None
    started_at: Optional[AwareDatetime] = None
    completed_at: Optional[AwareDatetime] = None
    elapsed_time: Optional[str] = None
    updated_at: Optional[AwareDatetime] = None


    # def model_dump_json(self, *args, **kwargs):
    #     kwargs.setdefault("exclude_none", True)
    #     kwargs.setdefault("exclude_unset", True)
    #     return super().model_dump_json(*args, **kwargs)


class EntryUploadStatus(StrEnum, Enum):
    STARTED = 'STARTED'
    COMPLETED = 'COMPLETED'
    ERROR = 'ERROR'
    class Config:  
        use_enum_values = True

class EntryUploadLog(CustomBaseModel):
    """Model for logging the upload status of a manifest entry."""
    entry_id: UUID
    status: EntryUploadStatus
    message: Optional[str] = None
    started_at: Optional[AwareDatetime] = None
    completed_at: Optional[AwareDatetime] = None
