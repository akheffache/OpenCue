"""
Host Simulator - Simulates render farm hosts by sending BootReport and HostReport
messages to CueBot via gRPC.
"""

import grpc
import time
import logging
from typing import List, Dict, Any, Optional

from opencue.compiled_proto import report_pb2
from opencue.compiled_proto import report_pb2_grpc
from opencue.compiled_proto import host_pb2

logger = logging.getLogger(__name__)


class SimulatedHost:
    """Represents a simulated render host."""

    def __init__(
        self,
        name: str,
        cores: int,
        memory_gb: float,
        facility: str = "local",
        tags: Optional[List[str]] = None,
    ):
        self.name = name
        self.cores = cores
        self.memory_kb = int(memory_gb * 1024 * 1024)  # Convert GB to KB
        self.facility = facility
        self.tags = tags or ["general"]

        # Track booked resources (updated when we get dispatch responses)
        self.booked_cores = 0
        self.booked_memory_kb = 0
        self.running_frames: List[report_pb2.RunningFrameInfo] = []

    def get_boot_report(self) -> report_pb2.BootReport:
        """Generate a BootReport for initial host registration."""
        render_host = report_pb2.RenderHost(
            name=self.name,
            nimby_enabled=False,
            nimby_locked=False,
            facility=self.facility,
            num_procs=1,
            cores_per_proc=self.cores,
            total_swap=self.memory_kb,
            total_mem=self.memory_kb,
            total_mcp=self.memory_kb,
            free_swap=self.memory_kb,
            free_mem=self.memory_kb,
            free_mcp=self.memory_kb,
            load=0,
            boot_time=int(time.time()),
            tags=self.tags,
            state=host_pb2.HardwareState.UP,
            attributes={},
            num_gpus=0,
            free_gpu_mem=0,
            total_gpu_mem=0,
        )

        core_detail = report_pb2.CoreDetail(
            total_cores=self.cores * 100,  # CueBot uses centicore units
            idle_cores=self.cores * 100,
            locked_cores=0,
            booked_cores=0,
        )

        return report_pb2.BootReport(
            host=render_host,
            core_info=core_detail,
        )

    def get_host_report(self) -> report_pb2.HostReport:
        """Generate a HostReport for status updates (triggers dispatch)."""
        idle_cores = (self.cores - self.booked_cores) * 100  # centicores
        idle_memory = self.memory_kb - self.booked_memory_kb

        render_host = report_pb2.RenderHost(
            name=self.name,
            nimby_enabled=False,
            nimby_locked=False,
            facility=self.facility,
            num_procs=1,
            cores_per_proc=self.cores,
            total_swap=self.memory_kb,
            total_mem=self.memory_kb,
            total_mcp=self.memory_kb,
            free_swap=idle_memory,
            free_mem=idle_memory,
            free_mcp=idle_memory,
            load=self.booked_cores * 100,  # Simulated load
            boot_time=int(time.time()) - 3600,  # Booted 1 hour ago
            tags=self.tags,
            state=host_pb2.HardwareState.UP,
            attributes={},
            num_gpus=0,
            free_gpu_mem=0,
            total_gpu_mem=0,
        )

        core_detail = report_pb2.CoreDetail(
            total_cores=self.cores * 100,
            idle_cores=idle_cores,
            locked_cores=0,
            booked_cores=self.booked_cores * 100,
        )

        return report_pb2.HostReport(
            host=render_host,
            frames=self.running_frames,
            core_info=core_detail,
        )

    @property
    def idle_cores(self) -> int:
        return self.cores - self.booked_cores

    @property
    def idle_memory_kb(self) -> int:
        return self.memory_kb - self.booked_memory_kb


class HostSimulator:
    """Manages a fleet of simulated hosts and their communication with CueBot."""

    def __init__(self, cuebot_host: str = "localhost", cuebot_port: int = 8443):
        self.cuebot_host = cuebot_host
        self.cuebot_port = cuebot_port
        self.hosts: Dict[str, SimulatedHost] = {}
        self._channel: Optional[grpc.Channel] = None
        self._stub: Optional[report_pb2_grpc.RqdReportInterfaceStub] = None

    def _get_stub(self) -> report_pb2_grpc.RqdReportInterfaceStub:
        """Get or create the gRPC stub."""
        if self._stub is None:
            self._channel = grpc.insecure_channel(
                f"{self.cuebot_host}:{self.cuebot_port}"
            )
            self._stub = report_pb2_grpc.RqdReportInterfaceStub(self._channel)
        return self._stub

    def create_hosts_from_config(self, host_config: Dict[str, Any], facility: str = "local") -> None:
        """Create simulated hosts from configuration dictionary."""
        for host_type, config in host_config.items():
            count = config["count"]
            cores = config["cores"]
            memory_gb = config["memory_gb"]
            tags = config.get("tags", ["general"])

            logger.info(f"Creating {count} {host_type} hosts ({cores} cores, {memory_gb}GB)")

            for i in range(count):
                name = f"{host_type}-{i:04d}"
                host = SimulatedHost(
                    name=name,
                    cores=cores,
                    memory_gb=memory_gb,
                    facility=facility,
                    tags=tags,
                )
                self.hosts[name] = host

        logger.info(f"Created {len(self.hosts)} total simulated hosts")

    def register_all_hosts(self, batch_size: int = 100, delay_between_batches: float = 0.1) -> int:
        """Send BootReport for all hosts to register them with CueBot."""
        stub = self._get_stub()
        registered = 0
        host_list = list(self.hosts.values())

        for i in range(0, len(host_list), batch_size):
            batch = host_list[i:i + batch_size]
            for host in batch:
                try:
                    request = report_pb2.RqdReportRqdStartupRequest(
                        boot_report=host.get_boot_report()
                    )
                    stub.ReportRqdStartup(request, timeout=10)
                    registered += 1
                except grpc.RpcError as e:
                    logger.error(f"Failed to register host {host.name}: {e}")

            if delay_between_batches > 0 and i + batch_size < len(host_list):
                time.sleep(delay_between_batches)

            logger.info(f"Registered {registered}/{len(self.hosts)} hosts")

        return registered

    def send_host_reports(self, batch_size: int = 100, delay_between_batches: float = 0.1) -> int:
        """Send HostReport for all hosts to trigger dispatch."""
        stub = self._get_stub()
        reported = 0
        host_list = list(self.hosts.values())

        for i in range(0, len(host_list), batch_size):
            batch = host_list[i:i + batch_size]
            for host in batch:
                try:
                    request = report_pb2.RqdReportStatusRequest(
                        host_report=host.get_host_report()
                    )
                    stub.ReportStatus(request, timeout=10)
                    reported += 1
                except grpc.RpcError as e:
                    logger.error(f"Failed to send report for host {host.name}: {e}")

            if delay_between_batches > 0 and i + batch_size < len(host_list):
                time.sleep(delay_between_batches)

        logger.debug(f"Sent {reported} host reports")
        return reported

    def get_total_cores(self) -> int:
        """Get total cores across all hosts."""
        return sum(h.cores for h in self.hosts.values())

    def get_total_memory_gb(self) -> float:
        """Get total memory across all hosts in GB."""
        return sum(h.memory_kb for h in self.hosts.values()) / 1024 / 1024

    def close(self) -> None:
        """Close the gRPC channel."""
        if self._channel:
            self._channel.close()
            self._channel = None
            self._stub = None
