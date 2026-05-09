# OpenCue Benchmarks

Performance benchmarks for OpenCue scheduling and dispatch.

## Available Benchmarks

### [fragmentation/](fragmentation/)

Measures scheduling fragmentation by simulating a render farm workload. Helps identify how much capacity is lost due to resource fragmentation when scheduling frames with varying core/memory requirements.

## Running Benchmarks

Each benchmark has its own README with specific instructions. Generally:

```bash
cd <benchmark_directory>
python simulation.py --help
```

## Requirements

- Running CueBot instance
- Python 3.7+
- OpenCue Python libraries (`pycue`, `pyoutline`)
