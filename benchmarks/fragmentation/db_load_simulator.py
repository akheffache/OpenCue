"""
DB Load Simulator - Simulates concurrent API queries to stress-test CueBot.

Runs in a background thread, continuously querying CueBot for jobs, layers,
frames, hosts, and procs to simulate real-world load from CueGUI users,
monitoring scripts, and other API consumers.
"""

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

import opencue

logger = logging.getLogger(__name__)


@dataclass
class LoadStats:
    """Statistics from the load simulator."""
    total_queries: int = 0
    successful_queries: int = 0
    failed_queries: int = 0
    queries_by_type: dict = field(default_factory=lambda: {
        "getJobs": 0,
        "getHosts": 0,
        "getLayers": 0,
        "getFrames": 0,
        "getProcs": 0,
    })
    total_time_sec: float = 0.0

    @property
    def queries_per_second(self) -> float:
        if self.total_time_sec == 0:
            return 0.0
        return self.total_queries / self.total_time_sec

    @property
    def success_rate(self) -> float:
        if self.total_queries == 0:
            return 0.0
        return (self.successful_queries / self.total_queries) * 100


class DBLoadSimulator:
    """
    Simulates concurrent database load by querying CueBot API.

    Runs in a background thread and performs various API queries at a
    configurable rate to simulate real-world usage patterns.
    """

    def __init__(
        self,
        queries_per_second: float = 10.0,
        job_query_weight: float = 0.3,
        host_query_weight: float = 0.2,
        layer_query_weight: float = 0.2,
        frame_query_weight: float = 0.2,
        proc_query_weight: float = 0.1,
    ):
        """
        Initialize the load simulator.

        Args:
            queries_per_second: Target QPS to generate
            job_query_weight: Probability of getJobs query (0.0-1.0)
            host_query_weight: Probability of getHosts query
            layer_query_weight: Probability of getLayers query
            frame_query_weight: Probability of getFrames query
            proc_query_weight: Probability of getProcs query
        """
        self.queries_per_second = queries_per_second
        self.query_weights = {
            "getJobs": job_query_weight,
            "getHosts": host_query_weight,
            "getLayers": layer_query_weight,
            "getFrames": frame_query_weight,
            "getProcs": proc_query_weight,
        }

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stats = LoadStats()
        self._lock = threading.Lock()

        # Cache of known jobs/layers for drilling down
        self._known_jobs: List[str] = []
        self._known_layers: List[str] = []

    def _select_query_type(self) -> str:
        """Randomly select a query type based on weights."""
        types = list(self.query_weights.keys())
        weights = list(self.query_weights.values())
        return random.choices(types, weights=weights, k=1)[0]

    def _execute_query(self, query_type: str) -> bool:
        """Execute a single query and return success status."""
        try:
            if query_type == "getJobs":
                jobs = opencue.api.getJobs()
                # Cache some job names for layer queries
                if jobs:
                    self._known_jobs = [j.name() for j in jobs[:10]]
                return True

            elif query_type == "getHosts":
                opencue.api.getHosts()
                return True

            elif query_type == "getLayers":
                if self._known_jobs:
                    job_name = random.choice(self._known_jobs)
                    try:
                        jobs = opencue.api.getJobs(job=[job_name])
                        if jobs:
                            layers = jobs[0].getLayers()
                            # Cache some layer info
                            if layers:
                                self._known_layers = [
                                    (job_name, l.name()) for l in layers[:5]
                                ]
                    except Exception:
                        pass
                return True

            elif query_type == "getFrames":
                if self._known_layers:
                    job_name, layer_name = random.choice(self._known_layers)
                    try:
                        jobs = opencue.api.getJobs(job=[job_name])
                        if jobs:
                            layers = jobs[0].getLayers()
                            for layer in layers:
                                if layer.name() == layer_name:
                                    layer.getFrames()
                                    break
                    except Exception:
                        pass
                return True

            elif query_type == "getProcs":
                opencue.api.getProcs()
                return True

            return False

        except Exception as e:
            logger.debug(f"Query {query_type} failed: {e}")
            return False

    def _run_loop(self) -> None:
        """Main loop that runs in background thread."""
        interval = 1.0 / self.queries_per_second if self.queries_per_second > 0 else 1.0
        start_time = time.time()

        logger.info(f"DB Load Simulator started ({self.queries_per_second} QPS)")

        while self._running:
            query_start = time.time()

            query_type = self._select_query_type()
            success = self._execute_query(query_type)

            with self._lock:
                self._stats.total_queries += 1
                self._stats.queries_by_type[query_type] += 1
                if success:
                    self._stats.successful_queries += 1
                else:
                    self._stats.failed_queries += 1

            # Sleep to maintain target QPS
            elapsed = time.time() - query_start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Record total runtime
        with self._lock:
            self._stats.total_time_sec = time.time() - start_time

        logger.info("DB Load Simulator stopped")

    def start(self) -> None:
        """Start the load simulator in a background thread."""
        if self._running:
            logger.warning("Load simulator already running")
            return

        self._running = True
        self._stats = LoadStats()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> LoadStats:
        """Stop the load simulator and return statistics."""
        if not self._running:
            return self._stats

        self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

        return self.get_stats()

    def get_stats(self) -> LoadStats:
        """Get current statistics (thread-safe)."""
        with self._lock:
            return LoadStats(
                total_queries=self._stats.total_queries,
                successful_queries=self._stats.successful_queries,
                failed_queries=self._stats.failed_queries,
                queries_by_type=dict(self._stats.queries_by_type),
                total_time_sec=self._stats.total_time_sec,
            )

    def print_stats(self, stats: Optional[LoadStats] = None) -> None:
        """Print formatted statistics."""
        if stats is None:
            stats = self.get_stats()

        print("\n--- DB LOAD SIMULATOR STATS ---")
        print(f"Total Queries:      {stats.total_queries}")
        print(f"Successful:         {stats.successful_queries}")
        print(f"Failed:             {stats.failed_queries}")
        print(f"Success Rate:       {stats.success_rate:.1f}%")
        print(f"Runtime:            {stats.total_time_sec:.1f}s")
        print(f"Actual QPS:         {stats.queries_per_second:.1f}")
        print("Queries by Type:")
        for query_type, count in stats.queries_by_type.items():
            print(f"  {query_type}: {count}")
