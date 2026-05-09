#!/usr/bin/env python
"""
Fragmentation Simulation - Main orchestrator for the render farm simulation.

This simulation measures fragmentation in OpenCue's scheduling by:
1. Registering simulated hosts (elks, rams, jaimes)
2. Submitting jobs with realistic frame distributions
3. Triggering dispatch via host reports
4. Measuring resulting fragmentation

Usage:
    python simulation.py [--cuebot HOST:PORT] [--utilization 0.2]
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any

import yaml

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import opencue
from opencue.cuebot import Cuebot

from host_simulator import HostSimulator
from job_generator import JobGenerator
from metrics import MetricsCollector

logger = logging.getLogger(__name__)


def load_config(config_dir: Path) -> Dict[str, Any]:
    """Load configuration from YAML files."""
    hosts_file = config_dir / "hosts.yaml"
    frames_file = config_dir / "frames.yaml"

    with open(hosts_file) as f:
        hosts_config = yaml.safe_load(f)

    with open(frames_file) as f:
        frames_config = yaml.safe_load(f)

    return {
        "hosts": hosts_config["hosts"],
        "frame_types": frames_config["frame_types"],
        "priorities": frames_config["priorities"],
        "target_utilization": frames_config.get("target_utilization", 0.20),
    }


def setup_show_and_facility(show_name: str, facility_name: str) -> bool:
    """Create show and facility if they don't exist."""
    try:
        # Check if show exists
        try:
            show = opencue.api.findShow(show_name)
            logger.info(f"Show '{show_name}' already exists")
        except opencue.CueException:
            logger.info(f"Creating show '{show_name}'")
            opencue.api.createShow(show_name)

        # Check if facility exists
        try:
            facilities = opencue.api.getFacilities()
            facility_names = [f.data.name for f in facilities]
            if facility_name not in facility_names:
                logger.info(f"Creating facility '{facility_name}'")
                opencue.api.createFacility(facility_name)
            else:
                logger.info(f"Facility '{facility_name}' already exists")
        except Exception as e:
            logger.warning(f"Could not check/create facility: {e}")

        return True
    except Exception as e:
        logger.error(f"Failed to setup show/facility: {e}")
        return False


def wait_for_dispatch_to_settle(
    job_names: list,
    max_rounds: int = 10,
    settle_threshold: int = 2,
) -> None:
    """Wait for dispatch to settle (no new frames being dispatched)."""
    logger.info("Waiting for dispatch to settle...")

    previous_running = 0
    stable_rounds = 0

    for round_num in range(max_rounds):
        time.sleep(2)

        total_running = 0
        for job_name in job_names:
            try:
                jobs = opencue.api.getJobs(job=[job_name])
                for job in jobs:
                    total_running += job.data.job_stats.running_frames
            except Exception:
                pass

        if total_running == previous_running:
            stable_rounds += 1
            if stable_rounds >= settle_threshold:
                logger.info(f"Dispatch settled after {round_num + 1} rounds")
                return
        else:
            stable_rounds = 0

        previous_running = total_running
        logger.info(f"Round {round_num + 1}: {total_running} frames running")

    logger.info(f"Reached max rounds ({max_rounds}), proceeding with measurement")


def run_simulation(
    cuebot_host: str = "localhost",
    cuebot_port: int = 8443,
    target_utilization: float = 0.20,
    show_name: str = "benchmark",
    facility_name: str = "local",
    config_dir: Path = None,
    skip_host_registration: bool = False,
) -> None:
    """Run the fragmentation simulation."""

    # Load configuration
    if config_dir is None:
        config_dir = Path(__file__).parent / "config"

    logger.info(f"Loading configuration from {config_dir}")
    config = load_config(config_dir)

    # Override target utilization if specified
    if target_utilization != config["target_utilization"]:
        config["target_utilization"] = target_utilization

    # Initialize CueBot connection
    logger.info(f"Connecting to CueBot at {cuebot_host}:{cuebot_port}")
    Cuebot.setHosts([f"{cuebot_host}:{cuebot_port}"])

    # Setup show and facility
    if not setup_show_and_facility(show_name, facility_name):
        logger.error("Failed to setup show/facility, aborting")
        return

    # Initialize components
    host_simulator = HostSimulator(cuebot_host, cuebot_port)
    job_generator = JobGenerator(
        show_name=show_name,
        frame_types=config["frame_types"],
        priorities=config["priorities"],
    )
    metrics_collector = MetricsCollector()

    try:
        # Phase 1: Create simulated hosts
        logger.info("=" * 50)
        logger.info("PHASE 1: Host Registration")
        logger.info("=" * 50)

        host_simulator.create_hosts_from_config(config["hosts"], facility=facility_name)
        total_cores = host_simulator.get_total_cores()
        total_memory_gb = host_simulator.get_total_memory_gb()

        logger.info(f"Total simulated cores: {total_cores}")
        logger.info(f"Total simulated memory: {total_memory_gb:.1f} GB")

        if not skip_host_registration:
            registered = host_simulator.register_all_hosts()
            logger.info(f"Registered {registered} hosts with CueBot")

            # Send initial host reports to make hosts available
            logger.info("Sending initial host reports...")
            host_simulator.send_host_reports()
            time.sleep(2)  # Let CueBot process
        else:
            logger.info("Skipping host registration (--skip-host-registration)")

        # Phase 2: Submit jobs
        logger.info("=" * 50)
        logger.info("PHASE 2: Job Submission")
        logger.info("=" * 50)

        target_cores = int(total_cores * config["target_utilization"])
        logger.info(f"Target utilization: {config['target_utilization']*100:.0f}%")
        logger.info(f"Target cores: {target_cores}")

        job_names = job_generator.create_jobs(target_cores)
        logger.info(f"Created {len(job_names)} jobs")

        if not job_names:
            logger.error("No jobs created, aborting")
            return

        # Phase 3: Dispatch loop
        logger.info("=" * 50)
        logger.info("PHASE 3: Dispatch Loop")
        logger.info("=" * 50)

        # Send host reports to trigger dispatch
        for dispatch_round in range(5):
            logger.info(f"Dispatch round {dispatch_round + 1}")
            host_simulator.send_host_reports()
            time.sleep(1)

        # Wait for dispatch to settle
        wait_for_dispatch_to_settle(job_names)

        # Phase 4: Collect and report metrics
        logger.info("=" * 50)
        logger.info("PHASE 4: Results")
        logger.info("=" * 50)

        report = metrics_collector.generate_report(job_names)
        metrics_collector.print_report(report)

        # Summary
        print("\n" + "=" * 70)
        print("SIMULATION SUMMARY")
        print("=" * 70)
        print(f"Target Utilization:   {config['target_utilization']*100:.0f}%")
        print(f"Target Cores:         {target_cores}")
        print(f"Actual Booked Cores:  {report.booked_cores:.0f}")
        print(f"Actual Utilization:   {report.core_utilization_percent:.1f}%")
        print(f"Fragmented Cores:     {report.fragmented_cores:.0f}")
        print(f"Fragmentation Rate:   {report.effective_fragmentation_percent:.1f}%")
        print("=" * 70)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        # Cleanup
        logger.info("Cleaning up...")
        host_simulator.close()

        # Optionally cleanup jobs (commented out to allow inspection)
        # job_generator.cleanup_jobs()


def main():
    parser = argparse.ArgumentParser(
        description="Run fragmentation simulation against CueBot"
    )
    parser.add_argument(
        "--cuebot",
        default="localhost:8443",
        help="CueBot host:port (default: localhost:8443)",
    )
    parser.add_argument(
        "--utilization",
        type=float,
        default=0.20,
        help="Target utilization (0.0-1.0, default: 0.20)",
    )
    parser.add_argument(
        "--show",
        default="benchmark",
        help="Show name to use (default: benchmark)",
    )
    parser.add_argument(
        "--facility",
        default="local",
        help="Facility name to use (default: local)",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=None,
        help="Path to config directory (default: ./config)",
    )
    parser.add_argument(
        "--skip-host-registration",
        action="store_true",
        help="Skip host registration (use existing hosts)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Parse cuebot host:port
    if ":" in args.cuebot:
        cuebot_host, cuebot_port = args.cuebot.split(":")
        cuebot_port = int(cuebot_port)
    else:
        cuebot_host = args.cuebot
        cuebot_port = 8443

    run_simulation(
        cuebot_host=cuebot_host,
        cuebot_port=cuebot_port,
        target_utilization=args.utilization,
        show_name=args.show,
        facility_name=args.facility,
        config_dir=args.config_dir,
        skip_host_registration=args.skip_host_registration,
    )


if __name__ == "__main__":
    main()
