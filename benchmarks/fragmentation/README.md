# Fragmentation Benchmark

This benchmark simulates a render farm workload to measure scheduling fragmentation in OpenCue.

## Overview

**Fragmentation** occurs when hosts have idle resources (cores/memory) that cannot be used because no pending frame fits the available slot. This benchmark:

1. Registers simulated hosts matching a real production farm
2. Submits jobs with realistic frame distributions
3. Measures how many cores become "stranded" due to fragmentation

## Farm Configuration

### Hosts (1,553 total)

| Type  | Count | Cores | Memory | Memory/Core |
|-------|-------|-------|--------|-------------|
| elk   | 1,004 | 16    | 125GB  | ~7.8GB      |
| ram   | 303   | 32    | 251GB  | ~7.8GB      |
| jaime | 246   | 128   | 503GB  | ~3.9GB      |

**Total: 57,248 cores**

### Frame Distribution

| Cores | % of Frames | Memory Required |
|-------|-------------|-----------------|
| 1     | 22.89%      | 0.52GB          |
| 2     | 25.54%      | 1.38GB          |
| 4     | 34.22%      | 6.44GB          |
| 8     | 14.55%      | 27.27GB         |
| 16    | 1.88%       | 63.85GB         |
| 32    | 0.28%       | 109GB           |
| 64    | 0.03%       | 233GB           |

## Usage

### Prerequisites

- Running CueBot instance
- Python 3.7+
- OpenCue Python libraries installed (`pycue`, `pyoutline`)

### Running the Simulation

```bash
# Basic run (20% utilization target)
python simulation.py --cuebot localhost:8443

# Higher utilization
python simulation.py --cuebot localhost:8443 --utilization 0.5

# Verbose output
python simulation.py --cuebot localhost:8443 -v

# Custom show/facility
python simulation.py --cuebot localhost:8443 --show myshow --facility myfacility
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--cuebot` | localhost:8443 | CueBot host:port |
| `--utilization` | 0.20 | Target utilization (0.0-1.0) |
| `--show` | benchmark | Show name |
| `--facility` | local | Facility name |
| `--config-dir` | ./config | Config directory path |
| `--skip-host-registration` | false | Use existing hosts |
| `-v, --verbose` | false | Enable debug logging |

## Output

The simulation produces a fragmentation report:

```
======================================================================
FRAGMENTATION REPORT
======================================================================

--- OVERALL METRICS ---
Total Hosts:        1553
Hosts with Frames:  800
Fully Utilized:     450
Fragmented Hosts:   150

Total Cores:        57248
Booked Cores:       11450
Idle Cores:         45798
Fragmented Cores:   2500

Core Utilization:   20.0%
Fragmentation:      5.5% of idle cores unusable
Effective Frag:     4.4% of total cores

--- BY HOST TYPE ---

ELK:
  Count:            1004
  Total Cores:      16064
  Booked Cores:     5000
  Utilization:      31.1%
  Fragmented Cores: 1200
  Fragmentation:    10.8%
...
```

## Architecture

```
simulation.py          # Main orchestrator
├── host_simulator.py  # BootReport/HostReport via gRPC
├── job_generator.py   # Create jobs with priorities
├── metrics.py         # Query CueBot, calculate fragmentation
└── config/
    ├── hosts.yaml     # Host specifications
    └── frames.yaml    # Frame distribution
```

## How It Works

### Phase 1: Host Registration
Sends gRPC `BootReport` messages to register simulated hosts with CueBot.

### Phase 2: Job Submission
Creates 5 jobs (one per priority level: 10, 30, 50, 70, 90), each containing layers for different core requirements.

### Phase 3: Dispatch Loop
Sends `HostReport` messages to trigger CueBot's dispatcher, which books frames to available hosts.

### Phase 4: Measurement
Queries CueBot for host state and calculates:
- **Booked cores**: Cores allocated to running frames
- **Idle cores**: Cores not allocated
- **Fragmented cores**: Idle cores where no pending frame fits

## Interpreting Results

- **High fragmentation** indicates the scheduling algorithm is leaving resources stranded
- Compare results with Redis cache enabled vs disabled
- Higher utilization targets will show more fragmentation

## Files

| File | Description |
|------|-------------|
| `simulation.py` | Main entry point |
| `host_simulator.py` | Simulates RQD hosts |
| `job_generator.py` | Creates benchmark jobs |
| `metrics.py` | Collects and reports metrics |
| `config/hosts.yaml` | Host configuration |
| `config/frames.yaml` | Frame distribution config |
