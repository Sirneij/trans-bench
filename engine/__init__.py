"""
engine: the benchmark engine of trans-bench.

loader.py reads the descriptors, runner.py runs one trial, run_one.py runs one trial in its own
process, and campaign.py runs a whole campaign of such processes. The descriptor classes are
exported here because almost every other module needs them.
"""

from engine.loader import (
    DescriptorLoader,
    GraphTypeDescriptor,
    SystemDescriptor,
    TimingPhase,
)

__all__ = [
    'DescriptorLoader',
    'SystemDescriptor',
    'GraphTypeDescriptor',
    'TimingPhase',
]
