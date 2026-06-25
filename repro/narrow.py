#!/usr/bin/env python3
"""Narrow UnalignedCheckpointRescaleITCase.getScaleFactors() to the single failing
parameter (downscale KEYED_BROADCAST 7 -> 2, sourceSleepMs = 0) so the repro loop runs
just that case. Usage: narrow.py <path-to-UnalignedCheckpointRescaleITCase.java>"""
import sys

path = sys.argv[1]
src = open(path).read()
start = src.index("new Object[][] {")
end = src.index("                };\n        return Arrays.stream(parameters)")
single = ('new Object[][] {\n'
          '                    new Object[] {"downscale", Topology.KEYED_BROADCAST, 7, 2, 0L},\n')
new = src[:start] + single + src[end:]
assert new != src and new.count("new Object[] {") == 1, "narrowing failed"
open(path, "w").write(new)
print("narrowed:", path)
