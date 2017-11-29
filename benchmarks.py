"""
Actual benchmarks
"""

def benchmark_list_databases(client, bench):
    return bench.bench_simple(lambda : client.get_all_databases())
