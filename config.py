from dataclasses import dataclass


@dataclass
class Config:
    BASE_DIR: str = "/ota-cache"
    CHUNK_SIZE: int = 2097_152  # in bytes


config = Config()
