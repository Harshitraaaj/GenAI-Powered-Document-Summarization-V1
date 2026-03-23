from pydantic import BaseModel, Field, model_validator
from typing import List, Literal
import logging

logger = logging.getLogger(__name__)


class ChunkSummary(BaseModel):
    chunk_id: int = Field(..., description="Unique identifier of the chunk")
    summary: str = Field(..., description="Detailed summary of the chunk")
    tldr: str = Field(..., description="One-line TLDR summary")
    key_points: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    action_items: List[str] = Field(default_factory=list)


class SectionSummary(BaseModel):
    section_id: int
    summary: str
    tldr: str
    key_points: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    action_items: List[str] = Field(default_factory=list)
    source_chunks: List[int] = Field(default_factory=list)


class ExecutiveSummary(BaseModel):
    tldr: str
    summary: str
    key_points: List[str] = Field(default_factory=list)
    risks: List[str] = Field(default_factory=list)
    action_items: List[str] = Field(default_factory=list)
    source_sections: List[int] = Field(default_factory=list)


class CoverageDetails(BaseModel):
    covered_chunk_ids: List[int] = Field(default_factory=list)
    missing_chunk_ids: List[int] = Field(default_factory=list)


class Metadata(BaseModel):
    total_chunks: int
    valid_chunks: int
    coverage_percent: float = Field(..., ge=0, le=100)
    missing_sections: int
    status: Literal[
        "ok",
        "low_coverage_warning",
        "section_missing_warning",
        "chunks_missing_warning"
    ]
    coverage_details: CoverageDetails


class SummarizationOutput(BaseModel):
    metadata: Metadata
    chunk_summaries: List[ChunkSummary]
    section_summaries: List[SectionSummary]
    executive_summary: ExecutiveSummary

    #Log successful validation
    @model_validator(mode="after")
    def log_validation_success(self):
        logger.info(
            f"Schema validation successful | "
            f"Chunks: {len(self.chunk_summaries)} | "
            f"Sections: {len(self.section_summaries)}"
        )
        return self