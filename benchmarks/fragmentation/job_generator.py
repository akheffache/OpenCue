"""
Job Generator - Creates jobs with layers matching the frame distribution.
"""

import logging
import random
from typing import List, Dict, Any, Optional

import opencue
from opencue.compiled_proto import job_pb2

logger = logging.getLogger(__name__)


class FrameType:
    """Represents a type of frame with specific resource requirements."""

    def __init__(self, cores: int, memory_gb: float, percent: float, tags: str = "general"):
        self.cores = cores
        self.memory_gb = memory_gb
        self.memory_kb = int(memory_gb * 1024 * 1024)
        self.percent = percent
        self.tags = tags  # Pipe-separated tags (e.g., "general" or "midrange|highend")


class JobGenerator:
    """Generates jobs with layers matching a specified frame distribution."""

    def __init__(
        self,
        show_name: str,
        frame_types: List[Dict[str, Any]],
        priorities: List[int],
    ):
        self.show_name = show_name
        self.frame_types = [
            FrameType(
                cores=ft["cores"],
                memory_gb=ft["memory_gb"],
                percent=ft["percent"],
                tags=ft.get("tags", "general"),
            )
            for ft in frame_types
        ]
        self.priorities = priorities
        self.created_jobs: List[str] = []

    def calculate_frame_counts(self, target_cores: int) -> Dict[int, int]:
        """
        Calculate how many frames of each type to create to reach target cores.

        Args:
            target_cores: Total number of cores worth of work to create

        Returns:
            Dict mapping core count to number of frames
        """
        frame_counts = {}

        for ft in self.frame_types:
            # Calculate cores allocated to this frame type
            cores_for_type = target_cores * (ft.percent / 100.0)
            # Calculate number of frames (round to int)
            num_frames = max(1, int(cores_for_type / ft.cores))
            frame_counts[ft.cores] = num_frames

        return frame_counts

    def _build_job_spec(
        self,
        job_name: str,
        priority: int,
        layers: List[Dict[str, Any]],
    ) -> str:
        """Build OpenCue job XML specification."""
        layers_xml = []

        for layer in layers:
            layer_xml = f"""
      <layer name="{layer['name']}" type="Render">
        <cmd>/bin/sleep 86400</cmd>
        <range>1-{layer['frame_count']}</range>
        <chunk>1</chunk>
        <cores>{layer['cores']}</cores>
        <memory>{layer['memory_kb']}</memory>
        <threadable>false</threadable>
        <tags>{layer['tags']}</tags>
      </layer>"""
            layers_xml.append(layer_xml)

        spec = f"""<?xml version="1.0"?>
<spec>
  <facility>local</facility>
  <show>{self.show_name}</show>
  <shot>benchmark</shot>
  <user>benchmark</user>
  <job name="{job_name}">
    <priority>{priority}</priority>
    <paused>false</paused>
    <maxretries>0</maxretries>
    <autoeat>true</autoeat>
    <layers>
      {"".join(layers_xml)}
    </layers>
  </job>
</spec>"""
        return spec

    def create_jobs(self, target_cores: int) -> List[str]:
        """
        Create jobs with frames distributed according to the frame type distribution.

        Creates one job per priority level, each containing layers for all frame types.
        Frames are distributed evenly across priority levels.

        Args:
            target_cores: Total cores worth of work to create

        Returns:
            List of created job names
        """
        frame_counts = self.calculate_frame_counts(target_cores)
        total_frames = sum(frame_counts.values())

        logger.info(f"Creating jobs for {target_cores} cores ({total_frames} total frames)")
        logger.info(f"Frame distribution: {frame_counts}")

        # Split frames across priority levels
        num_priorities = len(self.priorities)
        jobs_created = []

        for priority_idx, priority in enumerate(self.priorities):
            job_name = f"benchmark_p{priority}_{random.randint(1000, 9999)}"
            layers = []

            for ft in self.frame_types:
                total_for_type = frame_counts[ft.cores]
                # Distribute frames across priorities
                frames_for_this_job = total_for_type // num_priorities
                # Add remainder to last priority
                if priority_idx == num_priorities - 1:
                    frames_for_this_job += total_for_type % num_priorities

                if frames_for_this_job > 0:
                    layer_name = f"layer_{ft.cores}core"
                    layers.append({
                        "name": layer_name,
                        "cores": ft.cores,
                        "memory_kb": ft.memory_kb,
                        "frame_count": frames_for_this_job,
                        "tags": ft.tags,
                    })

            if layers:
                spec = self._build_job_spec(job_name, priority, layers)
                try:
                    logger.info(f"Launching job {job_name} with priority {priority}")
                    launched = opencue.api.launchSpecAndWait(spec)
                    if launched:
                        jobs_created.append(job_name)
                        logger.info(f"Created job: {job_name}")
                except Exception as e:
                    logger.error(f"Failed to create job {job_name}: {e}")

        self.created_jobs.extend(jobs_created)
        return jobs_created

    def get_pending_frame_types(self) -> List[FrameType]:
        """Get frame types that might still have pending frames."""
        return self.frame_types.copy()

    def cleanup_jobs(self) -> None:
        """Kill and delete all created jobs."""
        for job_name in self.created_jobs:
            try:
                jobs = opencue.api.getJobs(job=[job_name])
                for job in jobs:
                    logger.info(f"Killing job: {job.name()}")
                    job.kill()
            except Exception as e:
                logger.warning(f"Failed to cleanup job {job_name}: {e}")
        self.created_jobs.clear()


def get_frame_type_for_cores(frame_types: List[FrameType], cores: int) -> Optional[FrameType]:
    """Find a frame type by core count."""
    for ft in frame_types:
        if ft.cores == cores:
            return ft
    return None
