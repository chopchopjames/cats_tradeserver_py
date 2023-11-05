# -*- coding: utf-8 -*-
"""
.. moduleauthor:: Zhixiong Ge<56582881@qq.com>
"""
from line_profiler import LineProfiler

import time
# from line_profiler import LineProfiler

def do_profile(follow=[]):
    def inner(func):
        def profiled_func(*args, **kwargs):
            profiler = LineProfiler()

            try:
                profiler.add_function(func)
                for f in follow:
                    profiler.add_function(f)
                profiler.enable_by_count()
                return func(*args, **kwargs)

            finally:
                profiler.print_stats()

        return profiled_func

    return inner

ACCUM_TIMES = {}
COUNTERS = {}

def timefunc(f):
    def f_timer(*args, **kwargs):
        start = time.time()
        result = f(*args, **kwargs)
        end = time.time()
        if f.__name__ not in ACCUM_TIMES:
            ACCUM_TIMES[f.__name__] = 0
            COUNTERS[f.__name__] = 0

        ACCUM_TIMES[f.__name__] += end - start
        COUNTERS[f.__name__] += 1
        if COUNTERS[f.__name__] % 10 == 0:
            print('*************average time taken for {}: {}*****************'.format(f.__name__, ACCUM_TIMES[f.__name__] / COUNTERS[f.__name__]))

        return result
    return f_timer


import linecache
import os
import tracemalloc


def display_top(snapshot, key_type='lineno', limit=10):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        # replace "/path/to/module/file.py" with "module/file.py"
        filename = os.sep.join(frame.filename.split(os.sep)[-2:])
        print("#%s: %s:%s: %.1f KiB"
              % (index, filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))

