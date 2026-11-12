"""Application settings loaded strictly from environment variables."""

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _default_seed_path() -> Path:
    repo_relative = Path(__file__).resolve().parents[2] / "topology" / "seed_topology.json"
    if repo_relative.exists():
        return repo_relative
    cwd_relative = Path.cwd() / "topology" / "seed_topology.json"
    if cwd_relative.exists():
        return cwd_relative
    return Path("/app/topology/seed_topology.json")


class Settings(BaseSettings):
    """Runtime configuration for API, ingester, RCA engine, and simulator."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    redis_url: str = Field(default="redis://localhost:6379/0")
    neo4j_uri: str = Field(default="bolt://localhost:7687")
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(min_length=1)

    mqtt_host: str = Field(default="localhost")
    mqtt_port: int = Field(default=1883, ge=1, le=65535)

    heartbeat_ttl_seconds: float = Field(default=8.0, gt=1.0)
    expiry_sweep_interval_seconds: float = Field(default=1.0, gt=0.1)
    rca_interval_seconds: float = Field(default=1.5, gt=0.2)
    dedup_ttl_seconds: int = Field(default=300, ge=10)

    gateway_coverage_threshold: float = Field(default=0.75, gt=0.0, le=1.0)
    passive_panel_coverage_threshold: float = Field(default=0.85, gt=0.0, le=1.0)

    simulator_interval_seconds: float = Field(default=2.0, gt=0.2)
    topology_seed_path: Path = Field(default_factory=_default_seed_path)

    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000, ge=1, le=65535)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached immutable application settings."""
    return Settings()
