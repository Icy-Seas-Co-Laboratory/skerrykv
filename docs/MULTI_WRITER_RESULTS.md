# Cumulative multi-writer benchmark comparison

Lane-count deltas use the same executable fingerprint. Prior-version comparisons are lane 1 only and require matching workload settings, duration, warmup, store cap, and platform; the version change is deliberate. Other historical studies are context only. No SLO is inferred.

## Lane scaling within the candidate binary

| Study | Workload settings | Seconds | Lane | Item ops/s | Request p99 ms | Change vs lane 1 | Rejected / unknown |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 1 | 26,506.1 | 0.22 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 2 | 26,263.5 | 0.22 | -0.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 4 | 26,266.0 | 0.22 | -0.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 8 | 26,594.9 | 0.22 | +0.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 1 | 1,317.8 | 40.13 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 2 | 960.9 | 49.37 | -27.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 4 | 872.9 | 62.41 | -33.8% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 8 | 732.6 | 75.76 | -44.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 1 | 714.2 | 40.55 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 2 | 616.5 | 52.45 | -13.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 4 | 363.5 | 92.82 | -49.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 8 | 249.3 | 149.11 | -65.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 1 | 185.5 | 32.22 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 2 | 153.6 | 50.33 | -17.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 4 | 102.2 | 229.83 | -44.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 8 | 110.1 | 85.54 | -40.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 1 | 49,780.7 | 1.35 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 2 | 49,700.4 | 1.33 | -0.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 4 | 48,107.6 | 1.24 | -3.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 8 | 50,253.1 | 1.35 | +0.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 1 | 55,964.6 | 0.76 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 2 | 55,552.8 | 0.79 | -0.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 4 | 55,371.7 | 0.78 | -1.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 8 | 55,333.6 | 0.78 | -1.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 1 | 4,858.7 | 474.60 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 2 | 3,432.6 | 926.72 | -29.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 4 | 2,006.6 | 1,285.35 | -58.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 8 | 1,513.7 | 1,457.60 | -68.8% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 1 | 2,508.4 | 39.45 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 2 | 1,723.8 | 52.75 | -31.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 4 | 1,428.0 | 83.33 | -43.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 8 | 1,003.6 | 111.26 | -60.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 1 | 646.2 | 40.99 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 2 | 581.3 | 55.38 | -10.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 4 | 388.9 | 65.44 | -39.8% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 8 | 457.2 | 59.49 | -29.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 1 | 371.3 | 27.31 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 2 | 305.7 | 48.66 | -17.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 4 | 226.9 | 83.15 | -38.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 8 | 133.2 | 707.91 | -64.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 1 | 4,146.4 | 41.05 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 2 | 3,606.2 | 52.23 | -13.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 4 | 2,187.8 | 104.38 | -47.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 8 | 1,776.2 | 131.57 | -57.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 3 | 1 | 2,014.9 | 46.75 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 3 | 2 | 1,444.4 | 261.55 | -28.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 3 | 4 | 991.4 | 117.17 | -50.8% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 3 | 8 | 665.3 | 318.30 | -67.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 3 | 1 | 7,314.5 | 41.58 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 3 | 2 | 6,435.1 | 50.77 | -12.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 3 | 4 | 4,794.3 | 105.32 | -34.5% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 3 | 8 | 2,424.2 | 226.30 | -66.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 1 | 36,798.3 | 0.32 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 2 | 36,987.1 | 0.32 | +0.5% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 4 | 36,823.5 | 0.32 | +0.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 8 | 37,067.7 | 0.32 | +0.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 1 | 1,286.0 | 47.80 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 2 | 1,128.9 | 52.85 | -12.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 4 | 646.7 | 112.71 | -49.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 8 | 427.7 | 172.30 | -66.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 3 | 1 | 48,934.5 | 2.52 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 3 | 2 | 51,908.1 | 2.42 | +6.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 3 | 4 | 50,563.7 | 2.47 | +3.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 3 | 8 | 49,573.1 | 2.51 | +1.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 1 | 4,125.5 | 3,907.07 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 2 | 3,766.2 | 4,263.63 | -8.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 4 | 2,262.9 | 7,066.81 | -45.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 8 | 1,458.5 | 9,586.84 | -64.6% | 0 / 2777 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 1 | 54,923.1 | 0.52 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 2 | 54,659.4 | 0.53 | -0.5% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 4 | 54,771.0 | 0.52 | -0.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 8 | 54,562.2 | 0.52 | -0.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 1 | 59,097.2 | 0.13 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 2 | 58,843.3 | 0.13 | -0.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 4 | 59,858.3 | 0.13 | +1.3% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 8 | 59,339.0 | 0.13 | +0.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 1 | 4,361.3 | 264.38 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 2 | 3,448.4 | 350.62 | -20.9% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4 | 1,607.9 | 711.62 | -63.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 8 | 1,160.9 | 881.29 | -73.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 1 | 58,186.0 | 0.25 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 2 | 57,303.5 | 0.26 | -1.5% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 4 | 58,148.0 | 0.25 | -0.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 8 | 58,210.3 | 0.25 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 1 | 46,153.7 | 0.58 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 2 | 46,626.3 | 0.57 | +1.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 4 | 46,267.7 | 0.58 | +0.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 8 | 46,078.2 | 0.57 | -0.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 1 | 4,755.8 | 930.81 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 2 | 4,007.5 | 1,118.05 | -15.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 4 | 2,307.0 | 1,861.80 | -51.5% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 8 | 1,516.7 | 2,849.24 | -68.1% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 3 | 1 | 55,838.1 | 1.42 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 3 | 2 | 55,192.2 | 1.43 | -1.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 3 | 4 | 54,611.4 | 1.46 | -2.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 3 | 8 | 54,894.3 | 1.45 | -1.7% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 1 | 4,812.1 | 1,720.42 | +0.0% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 2 | 3,937.0 | 2,132.38 | -18.2% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 4 | 2,096.1 | 3,843.39 | -56.4% | 0 / 0 |
| target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 8 | 1,472.4 | 5,433.55 | -69.4% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=skew-put | 3 | 1 | 1,831.2 | 50.18 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=skew-put | 3 | 2 | 924.8 | 265.53 | -49.5% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=skew-put | 3 | 4 | 1,085.8 | 84.04 | -40.7% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=skew-put | 3 | 8 | 1,032.1 | 83.57 | -43.6% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=skew-put | 3 | 1 | 1,253.8 | 45.57 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=skew-put | 3 | 2 | 799.9 | 72.78 | -36.2% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=skew-put | 3 | 4 | 757.2 | 72.79 | -39.6% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=skew-put | 3 | 8 | 773.1 | 74.30 | -38.3% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=duplicate-put | 3 | 1 | 50,591.6 | 1.59 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=duplicate-put | 3 | 2 | 50,618.2 | 1.57 | +0.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=duplicate-put | 3 | 4 | 50,988.4 | 1.56 | +0.8% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=duplicate-put | 3 | 8 | 50,648.3 | 1.56 | +0.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=duplicate-put | 3 | 1 | 39,014.6 | 0.17 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=duplicate-put | 3 | 2 | 39,145.1 | 0.17 | +0.3% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=duplicate-put | 3 | 4 | 38,969.2 | 0.18 | -0.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=duplicate-put | 3 | 8 | 38,964.2 | 0.17 | -0.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=skew-put | 3 | 1 | 171.8 | 108.09 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=skew-put | 3 | 2 | 101.4 | 208.77 | -41.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=skew-put | 3 | 4 | 109.9 | 88.87 | -36.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=skew-put | 3 | 8 | 111.0 | 61.26 | -35.4% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=duplicate-put | 3 | 1 | 49,676.8 | 2.54 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=duplicate-put | 3 | 2 | 51,209.9 | 2.49 | +3.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=duplicate-put | 3 | 4 | 49,624.9 | 2.55 | -0.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=duplicate-put | 3 | 8 | 50,218.8 | 2.54 | +1.1% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=duplicate-put | 3 | 1 | 49,918.9 | 1.92 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=duplicate-put | 3 | 2 | 50,252.0 | 1.90 | +0.7% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=duplicate-put | 3 | 4 | 50,521.1 | 1.89 | +1.2% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=duplicate-put | 3 | 8 | 50,841.2 | 1.90 | +1.8% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=duplicate-put | 3 | 1 | 46,022.8 | 1.46 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=duplicate-put | 3 | 2 | 46,029.1 | 1.44 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=duplicate-put | 3 | 4 | 45,832.5 | 1.45 | -0.4% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=duplicate-put | 3 | 8 | 45,609.9 | 1.45 | -0.9% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=skew-put | 3 | 1 | 373.4 | 33.23 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=skew-put | 3 | 2 | 225.1 | 54.07 | -39.7% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=skew-put | 3 | 4 | 196.2 | 210.17 | -47.5% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=skew-put | 3 | 8 | 224.0 | 62.29 | -40.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=skew-put | 3 | 1 | 686.9 | 43.30 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=skew-put | 3 | 2 | 432.8 | 55.08 | -37.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=skew-put | 3 | 4 | 433.0 | 69.98 | -37.0% | 0 / 0 |
| target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=skew-put | 3 | 8 | 413.5 | 73.32 | -39.8% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=16, workload=batch-put | 3 | 1 | 3,439.0 | 121.98 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=16, workload=batch-put | 3 | 2 | 3,014.5 | 152.45 | -12.3% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=16, workload=batch-put | 3 | 4 | 1,659.8 | 231.24 | -51.7% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=16, workload=batch-put | 3 | 8 | 1,296.7 | 386.01 | -62.3% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 1 | 3,461.6 | 98.47 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 2 | 2,658.8 | 360.15 | -23.2% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 4 | 1,621.4 | 236.69 | -53.2% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 8 | 974.0 | 427.61 | -71.9% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 1 | 3,341.8 | 531.54 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 2 | 3,247.0 | 409.02 | -2.8% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 4 | 1,873.9 | 657.54 | -43.9% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 8 | 1,362.8 | 947.12 | -59.2% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 1 | 4,530.4 | 953.38 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 2 | 3,419.6 | 1,331.52 | -24.5% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 4 | 1,980.4 | 2,108.78 | -56.3% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 8 | 1,523.4 | 2,738.16 | -66.4% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 1 | 1,316.7 | 55.02 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 2 | 1,068.6 | 186.82 | -18.8% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 4 | 640.6 | 141.39 | -51.3% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 8 | 460.0 | 243.49 | -65.1% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=16, workload=batch-put | 3 | 1 | 4,687.9 | 265.44 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=16, workload=batch-put | 3 | 2 | 4,008.4 | 307.63 | -14.5% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=16, workload=batch-put | 3 | 4 | 1,919.5 | 777.69 | -59.1% | 0 / 0 |
| target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=16, workload=batch-put | 3 | 8 | 1,445.9 | 925.81 | -69.2% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 1 | 10,838.3 | 1,711.58 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 2 | 9,124.9 | 1,801.50 | -15.8% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 4 | 5,937.3 | 2,797.35 | -45.2% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 8 | 4,591.3 | 3,483.70 | -57.6% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 1 | 10,480.1 | 398.59 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 2 | 7,837.2 | 795.39 | -25.2% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 4 | 4,371.4 | 1,116.65 | -58.3% | 0 / 0 |
| target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 8 | 3,624.3 | 1,245.40 | -65.4% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 1 | 4,863.2 | 1,704.34 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 2 | 4,095.4 | 2,015.56 | -15.8% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 4 | 2,095.5 | 3,839.45 | -56.9% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 8 | 1,378.3 | 5,807.64 | -71.7% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 1 | 4,527.7 | 261.16 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 2 | 3,645.8 | 323.71 | -19.5% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4 | 2,241.4 | 520.62 | -50.5% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 8 | 1,415.4 | 784.25 | -68.7% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 1 | 4,809.0 | 927.65 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 2 | 4,254.8 | 1,010.85 | -11.5% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 4 | 2,364.3 | 1,790.60 | -50.8% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 8 | 1,386.0 | 2,985.25 | -71.2% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 1 | 4,162.5 | 805.76 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 2 | 4,172.4 | 568.24 | +0.2% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 4 | 2,168.5 | 1,187.78 | -47.9% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 8 | 1,355.1 | 1,749.65 | -67.4% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 1 | 4,508.5 | 3,542.62 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 2 | 3,744.6 | 4,270.36 | -16.9% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 4 | 2,148.5 | 7,440.97 | -52.3% | 0 / 0 |
| target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 8 | 1,556.7 | 9,938.44 | -65.5% | 0 / 1028 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 1 | 3,642.0 | 346.28 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 2 | 2,891.7 | 622.61 | -20.6% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 4 | 1,753.7 | 775.25 | -51.8% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 8 | 1,281.4 | 988.76 | -64.8% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 1 | 1,226.9 | 177.20 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 2 | 1,135.7 | 85.40 | -7.4% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 4 | 607.4 | 152.00 | -50.5% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 8 | 444.1 | 233.16 | -63.8% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 1 | 3,424.8 | 194.11 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 2 | 2,962.5 | 118.63 | -13.5% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 4 | 1,480.8 | 221.86 | -56.8% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 8 | 1,070.8 | 321.61 | -68.7% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 1 | 4,270.8 | 1,241.86 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 2 | 3,761.5 | 1,211.42 | -11.9% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 4 | 1,825.3 | 2,406.19 | -57.3% | 0 / 0 |
| target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 8 | 1,371.8 | 3,111.88 | -67.9% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 1 | 11,189.2 | 1,508.89 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 2 | 9,181.3 | 1,788.95 | -17.9% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 4 | 5,366.1 | 3,309.61 | -52.0% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 8 | 4,257.1 | 3,784.71 | -62.0% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 1 | 10,564.4 | 401.41 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 2 | 7,562.8 | 806.41 | -28.4% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 4 | 5,304.3 | 782.32 | -49.8% | 0 / 0 |
| target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 8 | 3,870.8 | 1,144.36 | -63.4% | 0 / 0 |
| target/multi-writer-0.4-long-alternating.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 10 | 1 | 1,639.1 | 166.04 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-long-alternating.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 10 | 1 | 4,570.4 | 3,492.89 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-long-alternating.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 10 | 1 | 7,313.8 | 43.55 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-delay2.json | batch_delay_ms=2, batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 10 | 1 | 2,353.3 | 50.06 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-delay2.json | batch_delay_ms=2, batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 10 | 1 | 4,680.0 | 3,937.34 | +0.0% | 0 / 0 |
| target/multi-writer-0.4-delay2.json | batch_delay_ms=2, batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 10 | 1 | 8,458.6 | 42.22 | +0.0% | 0 / 0 |

## Matched prior-version lane-1 comparison

Only matched candidate lane-1 rows are compared with the selected prior-version aggregate. Binary fingerprints differ by design and are listed in JSON.

| Baseline source | Candidate source | Workload settings | Seconds | Prior item ops/s | Candidate item ops/s | Change | Prior / candidate p99 ms | Prior / candidate rejected / unknown |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 26,314.2 | 26,506.1 | +0.7% | 0.22 / 0.22 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 4,947.4 | 4,858.7 | -1.8% | 463.32 / 474.60 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 2,552.2 | 2,508.4 | -1.7% | 40.52 / 39.45 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 49,630.3 | 49,780.7 | +0.3% | 1.36 / 1.35 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 288.6 | 371.3 | +28.7% | 298.57 / 27.31 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 3 | 2,712.8 | 2,014.9 | -25.7% | 44.56 / 46.75 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 4,471.0 | 4,146.4 | -7.3% | 42.81 / 41.05 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 59,250.5 | 59,097.2 | -0.3% | 0.13 / 0.13 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4,759.1 | 4,361.3 | -8.4% | 253.01 / 264.38 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 55,117.9 | 55,964.6 | +1.5% | 0.78 / 0.76 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 185.0 | 185.5 | +0.3% | 31.60 / 32.22 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 656.9 | 714.2 | +8.7% | 184.12 / 40.55 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 55,209.0 | 58,186.0 | +5.4% | 0.27 / 0.25 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 3 | 52,515.9 | 48,934.5 | -6.8% | 2.41 / 2.52 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 1,337.6 | 1,286.0 | -3.9% | 106.75 / 47.80 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 3 | 56,050.9 | 55,838.1 | -0.4% | 1.38 / 1.42 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 703.1 | 646.2 | -8.1% | 39.28 / 40.99 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 5,185.9 | 4,812.1 | -7.2% | 1,573.88 / 1,720.42 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 36,442.1 | 36,798.3 | +1.0% | 0.34 / 0.32 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 56,232.6 | 54,923.1 | -2.3% | 0.48 / 0.52 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 4,711.2 | 4,125.5 | -12.4% | 3,409.92 / 3,907.07 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 1,251.9 | 1,317.8 | +5.3% | 40.95 / 40.13 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 4,910.1 | 4,755.8 | -3.1% | 848.86 / 930.81 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 3 | 8,474.1 | 7,314.5 | -13.7% | 42.55 / 41.58 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4.json | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 45,642.2 | 46,153.7 | +1.1% | 0.58 / 0.58 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=skew-put | 3 | 1,228.6 | 1,253.8 | +2.1% | 193.63 / 45.57 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=duplicate-put | 3 | 38,428.1 | 39,014.6 | +1.5% | 0.18 / 0.17 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=32, workload=duplicate-put | 3 | 51,380.1 | 49,918.9 | -2.8% | 2.12 / 1.92 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=4, workload=skew-put | 3 | 174.3 | 171.8 | -1.5% | 35.58 / 108.09 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=skew-put | 3 | 366.7 | 373.4 | +1.8% | 36.25 / 33.23 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=duplicate-put | 3 | 53,804.8 | 49,676.8 | -7.7% | 2.69 / 2.54 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=8, workload=duplicate-put | 3 | 45,255.7 | 46,022.8 | +1.7% | 1.69 / 1.46 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=64, workload=skew-put | 3 | 2,325.5 | 1,831.2 | -21.3% | 47.75 / 50.18 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=duplicate-put | 3 | 50,215.7 | 50,591.6 | +0.7% | 1.94 / 1.59 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-stress-matched.json: | target/multi-writer-0.4-stress.json | batch_items=1, fixture_items=1, payload_bytes=4096, workers=16, workload=skew-put | 3 | 726.6 | 686.9 | -5.5% | 29.25 / 43.30 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=16, workload=batch-put | 3 | 2,511.9 | 3,439.0 | +36.9% | 240.74 / 121.98 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 3,257.1 | 3,461.6 | +6.3% | 92.06 / 98.47 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 4,492.1 | 4,530.4 | +0.9% | 971.71 / 953.38 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 1,240.9 | 1,316.7 | +6.1% | 63.55 / 55.02 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=16, workload=batch-put | 3 | 4,543.9 | 4,687.9 | +3.2% | 249.62 / 265.44 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 2,786.5 | 3,341.8 | +19.9% | 463.26 / 531.54 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-admission.json: | target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 11,046.5 | 10,838.3 | -1.9% | 1,528.59 / 1,711.58 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-admission.json: | target/multi-writer-0.4-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 11,048.0 | 10,480.1 | -5.1% | 391.55 / 398.59 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 5,185.9 | 4,863.2 | -6.2% | 1,573.88 / 1,704.34 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4,759.1 | 4,527.7 | -4.9% | 253.01 / 261.16 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 4,910.1 | 4,809.0 | -2.1% | 848.86 / 927.65 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 3 | 4,711.2 | 4,508.5 | -4.3% | 3,409.92 / 3,542.62 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-lane1-matched.json: | target/multi-writer-0.4-refill-batch.json | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 4,947.4 | 4,162.5 | -15.9% | 463.32 / 805.76 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=4, workload=batch-put | 3 | 1,240.9 | 1,226.9 | -1.1% | 63.55 / 177.20 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-refill-buffered.json | batch_items=16, client_memory_mib=1024, payload_bytes=131072, workers=64, workload=batch-put | 3 | 2,786.5 | 3,642.0 | +30.7% | 463.26 / 346.28 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=4, workload=batch-put | 3 | 3,257.1 | 3,424.8 | +5.1% | 92.06 / 194.11 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-buffered-matched.json: | target/multi-writer-0.4-refill-buffered.json | batch_items=64, client_memory_mib=1024, payload_bytes=32768, workers=64, workload=batch-put | 3 | 4,492.1 | 4,270.8 | -4.9% | 971.71 / 1,241.86 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-admission.json: | target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=64, workload=batch-put | 3 | 11,046.5 | 11,189.2 | +1.3% | 1,528.59 / 1,508.89 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-admission.json: | target/multi-writer-0.4-refill-admission.json | batch_items=250, max_requests=512, payload_bytes=4096, workers=16, workload=batch-put | 3 | 11,048.0 | 10,564.4 | -4.4% | 391.55 / 401.41 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-long-alternating.json: | target/multi-writer-0.4-long-alternating.json | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 10 | 2,218.7 | 1,639.1 | -26.1% | 51.66 / 166.04 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-long-alternating.json: | target/multi-writer-0.4-long-alternating.json | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 10 | 4,836.5 | 4,570.4 | -5.5% | 3,587.77 / 3,492.89 | 0 / 0 ; 0 / 0 |
| target/multi-writer-0.3-long-alternating.json: | target/multi-writer-0.4-long-alternating.json | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 10 | 7,781.4 | 7,313.8 | -6.0% | 43.69 / 43.55 | 0 / 0 ; 0 / 0 |

## Historical HTTP studies

These rows remain separate by source study and binary. They are descriptive and do not contribute to candidate lane deltas.

| Study | Case | Settings | Seconds | Item ops/s | Request p99 ms | Status / metrics unavailable | Rejected / unknown |
| --- | --- | --- | ---: | ---: | ---: | --- | ---: |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:additional_reports-1 | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 10 | 358.1 | 28.20 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:additional_reports-2 | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 10 | 345.4 | 41.09 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | batch-c16 | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 5,153.4 | 862.85 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | batch-c32 | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 4,939.5 | 1,882.21 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | batch-c4 | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4,698.1 | 306.48 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | batch-c8 | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 5,040.2 | 435.68 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | hot-get-c16 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 54,400.2 | 0.52 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | hot-get-c32 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 55,714.9 | 0.76 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | hot-get-c4 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 58,468.4 | 0.13 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | hot-get-c8 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 58,099.3 | 0.25 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | mixed-c16 | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 2,262.1 | 41.83 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | mixed-c32 | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 4,905.6 | 40.31 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | mixed-c4 | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 655.4 | 40.73 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | mixed-c8 | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 1,315.5 | 40.20 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | random-get-c16 | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 47,222.9 | 0.55 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | random-get-c32 | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 49,871.0 | 1.24 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | random-get-c4 | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 26,206.6 | 0.22 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | random-get-c8 | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 36,522.6 | 0.32 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | single-c16 | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 722.4 | 29.73 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | single-c32 | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 1,380.3 | 47.42 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | single-c4 | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 187.4 | 27.83 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:after | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 328.7 | 164.80 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | batch-c16 | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 5,164.1 | 835.13 | 6.7% / 6.7% available | 5500 / 1250 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | batch-c32 | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 5,083.3 | 1,631.48 | 6.7% / 6.7% available | 11000 / 5250 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | batch-c4 | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4,610.2 | 542.57 | 6.7% / 6.7% available | 250 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | batch-c8 | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 5,298.6 | 420.63 | 6.7% / 6.7% available | 1750 / 250 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | hot-get-c16 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 57,553.1 | 0.50 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | hot-get-c32 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 56,876.6 | 0.78 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | hot-get-c4 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 59,954.2 | 0.13 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | hot-get-c8 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 59,858.2 | 0.25 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | mixed-c16 | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 2,560.8 | 41.66 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | mixed-c32 | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 4,525.7 | 42.66 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | mixed-c4 | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 694.6 | 40.28 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | mixed-c8 | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 1,389.7 | 40.44 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | random-get-c16 | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 47,547.7 | 0.55 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | random-get-c32 | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 50,194.5 | 1.34 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | random-get-c4 | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 26,301.3 | 0.22 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | random-get-c8 | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 37,239.8 | 0.32 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | single-c16 | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 727.7 | 29.63 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | single-c32 | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 1,413.6 | 38.33 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | single-c4 | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 171.3 | 115.17 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:before | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 362.7 | 25.69 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache1024-c16 | batch_items=1, cache_mib=1024, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=16, workload=random-get | 3 | 50,941.7 | 0.42 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache1024-c32 | batch_items=1, cache_mib=1024, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=32, workload=random-get | 3 | 49,726.5 | 0.80 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache1024-c4 | batch_items=1, cache_mib=1024, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=4, workload=random-get | 3 | 44,066.5 | 0.15 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache1024-c8 | batch_items=1, cache_mib=1024, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=8, workload=random-get | 3 | 50,574.3 | 0.27 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache256-c16 | batch_items=1, cache_mib=256, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=16, workload=random-get | 3 | 46,732.9 | 0.60 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache256-c32 | batch_items=1, cache_mib=256, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=32, workload=random-get | 3 | 47,458.5 | 1.35 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache256-c4 | batch_items=1, cache_mib=256, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=4, workload=random-get | 3 | 27,297.9 | 0.24 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache256-c8 | batch_items=1, cache_mib=256, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=8, workload=random-get | 3 | 39,761.3 | 0.33 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache4096-c16 | batch_items=1, cache_mib=4096, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=16, workload=random-get | 3 | 48,658.6 | 0.49 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache4096-c32 | batch_items=1, cache_mib=4096, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=32, workload=random-get | 3 | 49,012.3 | 0.82 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache4096-c4 | batch_items=1, cache_mib=4096, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=4, workload=random-get | 3 | 44,608.1 | 0.14 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache4096-c8 | batch_items=1, cache_mib=4096, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=8, workload=random-get | 3 | 51,200.3 | 0.27 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache8192-c16 | batch_items=1, cache_mib=8192, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=16, workload=random-get | 3 | 49,766.7 | 0.45 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache8192-c32 | batch_items=1, cache_mib=8192, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=32, workload=random-get | 3 | 50,017.0 | 0.81 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache8192-c4 | batch_items=1, cache_mib=8192, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=4, workload=random-get | 3 | 43,801.9 | 0.15 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:cache | cache8192-c8 | batch_items=1, cache_mib=8192, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=8, workload=random-get | 3 | 51,230.4 | 0.28 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:followup_after | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 10 | 354.4 | 29.86 | 100.0% / 100.0% available | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:followup_before | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 10 | 351.9 | 29.28 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:after | batch-c64 | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 10 | 4,964.0 | 3,340.91 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:after | hot-get-c64 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 10 | 54,905.7 | 1.42 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:after | mixed-c64 | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 10 | 7,944.5 | 42.49 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:after | random-get-c64 | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 10 | 51,587.5 | 2.46 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:after | single-c64 | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 10 | 2,278.4 | 48.95 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:before | batch-c64 | batch_items=250, payload_bytes=4096, workers=64, workload=batch-put | 10 | 4,809.4 | 3,721.65 | 0.7% / 0.7% available | 132250 / 23000 |
| docs/HTTP_64_WORKER_RESULTS.json:before | hot-get-c64 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=64, workload=hot-get | 10 | 57,402.9 | 1.36 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:before | mixed-c64 | batch_items=1, payload_bytes=32768, workers=64, workload=mixed | 10 | 8,274.8 | 42.20 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:before | random-get-c64 | batch_items=1, payload_bytes=32768, workers=64, workload=random-get | 10 | 54,335.8 | 2.36 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:before | single-c64 | batch_items=1, payload_bytes=4096, workers=64, workload=unique-put | 10 | 2,361.6 | 47.27 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:cache | cache1024-c64 | batch_items=1, cache_mib=1024, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=64, workload=random-get | 3 | 48,370.4 | 1.91 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:cache | cache256-c64 | batch_items=1, cache_mib=256, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=64, workload=random-get | 3 | 50,991.7 | 1.87 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:cache | cache4096-c64 | batch_items=1, cache_mib=4096, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=64, workload=random-get | 3 | 49,388.2 | 1.54 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:cache | cache8192-c64 | batch_items=1, cache_mib=8192, client_memory_mib=1024, fixture_items=16384, payload_bytes=32768, workers=64, workload=random-get | 3 | 50,580.5 | 1.54 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | random-get-c4 | batch_items=1, payload_bytes=32768, workers=4, workload=random-get | 3 | 26,705.5 | 0.21 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | single-c4 | batch_items=1, payload_bytes=4096, workers=4, workload=unique-put | 3 | 192.8 | 30.71 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | mixed-c8 | batch_items=1, payload_bytes=32768, workers=8, workload=mixed | 3 | 1,426.6 | 40.34 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | single-c32 | batch_items=1, payload_bytes=4096, workers=32, workload=unique-put | 3 | 1,252.5 | 161.36 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | mixed-c4 | batch_items=1, payload_bytes=32768, workers=4, workload=mixed | 3 | 685.4 | 41.62 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | random-get-c8 | batch_items=1, payload_bytes=32768, workers=8, workload=random-get | 3 | 37,694.7 | 0.31 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | batch-c8 | batch_items=250, payload_bytes=4096, workers=8, workload=batch-put | 3 | 5,141.0 | 434.43 | 6.7% / 6.7% available | 1500 / 500 |
| docs/HTTP_CAPACITY_RESULTS.json | random-get-c32 | batch_items=1, payload_bytes=32768, workers=32, workload=random-get | 3 | 49,952.8 | 1.25 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | single-c8 | batch_items=1, payload_bytes=4096, workers=8, workload=unique-put | 3 | 324.6 | 153.82 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | batch-c16 | batch_items=250, payload_bytes=4096, workers=16, workload=batch-put | 3 | 5,367.2 | 794.48 | 6.7% / 6.7% available | 3250 / 1500 |
| docs/HTTP_CAPACITY_RESULTS.json | hot-get-c32 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=32, workload=hot-get | 3 | 57,638.2 | 0.75 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | mixed-c16 | batch_items=1, payload_bytes=32768, workers=16, workload=mixed | 3 | 2,442.7 | 41.34 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | hot-get-c4 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=4, workload=hot-get | 3 | 60,188.9 | 0.13 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | single-c16 | batch_items=1, payload_bytes=4096, workers=16, workload=unique-put | 3 | 732.4 | 29.58 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | batch-c4 | batch_items=250, payload_bytes=4096, workers=4, workload=batch-put | 3 | 4,729.6 | 509.76 | 6.7% / 6.7% available | 1750 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | hot-get-c16 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=16, workload=hot-get | 3 | 56,596.7 | 0.51 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | mixed-c32 | batch_items=1, payload_bytes=32768, workers=32, workload=mixed | 3 | 4,807.0 | 39.79 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | batch-c32 | batch_items=250, payload_bytes=4096, workers=32, workload=batch-put | 3 | 4,666.1 | 1,848.58 | 6.7% / 6.7% available | 14750 / 5250 |
| docs/HTTP_CAPACITY_RESULTS.json | random-get-c16 | batch_items=1, payload_bytes=32768, workers=16, workload=random-get | 3 | 46,892.1 | 0.56 | 100.0% / 100.0% available | 0 / 0 |
| docs/HTTP_CAPACITY_RESULTS.json | hot-get-c8 | batch_items=1, cache_mib=64, payload_bytes=4096, workers=8, workload=hot-get | 3 | 60,835.9 | 0.24 | 100.0% / 100.0% available | 0 / 0 |

## Additional historical benchmark result sets

The source JSON files and every summary row are retained in the JSON artifact. The compact table below groups the original rows by study, profile, and phase; it reports medians only within each source study, in that study's original units.

| Study | Profile | Phase | Rows | Datasets | Write patterns | Median ops/s | Median p99 µs | Latency definition |
| --- | --- | --- | ---: | --- | --- | ---: | ---: | --- |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | mixed_read_write | 2 | 20k | concurrent, sequential | 2,230.8 | 20,336.1 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | read_batch_ordered64 | 2 | 20k | concurrent, sequential | 2,357.9 | 6,782.2 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | read_batch_unordered64 | 2 | 20k | concurrent, sequential | 3,000.1 | 13,547.7 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | read_concurrent_random | 2 | 20k | concurrent, sequential | 3,782.1 | 2,943.8 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | read_hot | 2 | 20k | concurrent, sequential | 5,274.2 | 1,478.7 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | read_random | 2 | 20k | concurrent, sequential | 2,313.8 | 6,788.1 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | read_sequential | 2 | 20k | concurrent, sequential | 2,270.6 | 6,903.4 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | write_concurrent | 1 | 20k | concurrent | 1,641.7 | 53,697.6 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | write_duplicate | 2 | 20k | concurrent, sequential | 2,533.7 | 1,507.0 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_full_verify | write_sequential | 1 | 20k | sequential | 985.6 | 5,354.3 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | mixed_read_write | 2 | 20k | concurrent, sequential | 161.0 | 393,269.1 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | read_batch_ordered64 | 2 | 20k | concurrent, sequential | 2,217.4 | 5,674.1 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | read_batch_unordered64 | 2 | 20k | concurrent, sequential | 2,819.7 | 14,427.9 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | read_concurrent_random | 2 | 20k | concurrent, sequential | 3,293.6 | 12,290.5 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | read_hot | 2 | 20k | concurrent, sequential | 5,607.3 | 910.5 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | read_random | 2 | 20k | concurrent, sequential | 2,302.1 | 5,611.8 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | read_sequential | 2 | 20k | concurrent, sequential | 2,364.8 | 5,447.5 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | write_concurrent | 1 | 20k | concurrent | 54.1 | 687,271.1 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | write_duplicate | 2 | 20k | concurrent, sequential | 1,495.7 | 1,184.9 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | kv2_macos_fullsync_verify | write_sequential | 1 | 20k | sequential | 36.4 | 140,169.7 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | mixed_read_write | 2 | 20k | concurrent, sequential | 461.1 | 74,035.3 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | read_batch_ordered64 | 2 | 20k | concurrent, sequential | 77,790.1 | 14.4 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | read_batch_unordered64 | 2 | 20k | concurrent, sequential | 240,651.2 | 52.0 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | read_concurrent_random | 2 | 20k | concurrent, sequential | 269,458.7 | 59.4 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | read_hot | 2 | 20k | concurrent, sequential | 81,225.6 | 14.2 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | read_random | 2 | 20k | concurrent, sequential | 77,292.0 | 15.9 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | read_sequential | 2 | 20k | concurrent, sequential | 77,141.1 | 14.9 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | write_concurrent | 1 | 20k | concurrent | 110.4 | 81,018.0 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | write_duplicate | 2 | 20k | concurrent, sequential | 98,819.4 | 12.9 | per_operation |
| benchmarks/results/fullsync-2026-10-26/summary.json | skerry_engine | write_sequential | 1 | 20k | sequential | 109.1 | 12,627.2 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | mixed_read_write | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,881.5 | 6,242.8 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | read_batch_ordered64 | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,155.5 | 596.6 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | read_batch_unordered64 | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,836.4 | 3,628.9 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | read_concurrent_random | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,863.7 | 3,576.0 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | read_hot | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 14,396.8 | 405.2 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | read_random | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,953.6 | 648.5 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | read_sequential | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,986.8 | 645.3 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | write_concurrent | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent | 1,941.8 | 54,431.8 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | write_duplicate | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 4,131.2 | 786.9 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_current | write_sequential | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | sequential | 876.3 | 1,620.5 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | mixed_read_write | 2 | 1m, 20k | sequential | 1,926.9 | 9,965.0 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | read_batch_ordered64 | 2 | 1m, 20k | sequential | 3,763.0 | 423.4 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | read_batch_unordered64 | 2 | 1m, 20k | sequential | 3,861.0 | 3,549.0 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | read_concurrent_random | 2 | 1m, 20k | sequential | 3,678.3 | 3,929.8 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | read_hot | 2 | 1m, 20k | sequential | 10,927.0 | 572.6 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | read_random | 2 | 1m, 20k | sequential | 1,960.0 | 861.8 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | read_sequential | 2 | 1m, 20k | sequential | 1,932.8 | 777.2 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | write_duplicate | 2 | 1m, 20k | sequential | 1,414.5 | 2,667.4 | per_operation |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | kv2_stream | write_sequential | 2 | 1m, 20k | sequential | 543.1 | 2,834.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | mixed_read_write | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,484.7 | 18,457.7 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | read_batch_ordered64 | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 1,807.7 | 8,653.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | read_batch_unordered64 | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,463.9 | 13,592.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | read_concurrent_random | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,024.1 | 3,783.0 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | read_hot | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,606.9 | 2,007.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | read_random | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 1,753.0 | 8,763.9 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | read_sequential | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 1,740.0 | 8,794.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | write_concurrent | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent | 1,476.9 | 55,557.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | write_duplicate | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 1,546.0 | 2,662.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_current | write_sequential | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | sequential | 736.7 | 8,822.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | mixed_read_write | 2 | 128k, 20k | sequential | 2,431.7 | 16,744.4 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | read_batch_ordered64 | 2 | 128k, 20k | sequential | 1,671.4 | 8,675.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | read_batch_unordered64 | 2 | 128k, 20k | sequential | 2,422.9 | 13,606.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | read_concurrent_random | 2 | 128k, 20k | sequential | 3,032.1 | 3,987.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | read_hot | 2 | 128k, 20k | sequential | 3,134.8 | 2,110.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | read_random | 2 | 128k, 20k | sequential | 1,575.4 | 9,011.7 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | read_sequential | 2 | 128k, 20k | sequential | 1,567.8 | 8,939.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | write_duplicate | 2 | 128k, 20k | sequential | 1,557.4 | 2,615.9 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_full_verify | write_sequential | 2 | 128k, 20k | sequential | 753.0 | 5,597.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | mixed_read_write | 2 | 1m, 20k | sequential | 1,787.6 | 27,336.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | read_batch_ordered64 | 2 | 1m, 20k | sequential | 1,686.8 | 8,349.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | read_batch_unordered64 | 2 | 1m, 20k | sequential | 2,442.1 | 15,925.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | read_concurrent_random | 2 | 1m, 20k | sequential | 2,735.4 | 4,080.0 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | read_hot | 2 | 1m, 20k | sequential | 3,106.7 | 2,191.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | read_random | 2 | 1m, 20k | sequential | 1,617.8 | 8,792.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | read_sequential | 2 | 1m, 20k | sequential | 1,583.2 | 9,024.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | write_duplicate | 2 | 1m, 20k | sequential | 786.0 | 3,311.0 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | kv2_stream | write_sequential | 2 | 1m, 20k | sequential | 423.7 | 10,439.4 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | mixed_read_write | 1 | 20k | concurrent | 1,173.0 | 22,011.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | read_batch_ordered64 | 1 | 20k | concurrent | 557,658.3 | 0.7 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | read_batch_unordered64 | 1 | 20k | concurrent | 465,146.1 | 18.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | read_concurrent_random | 1 | 20k | concurrent | 430,591.3 | 21.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | read_hot | 1 | 20k | concurrent | 932,498.6 | 0.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | read_random | 1 | 20k | concurrent | 517,019.5 | 0.7 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | read_sequential | 1 | 20k | concurrent | 638,172.0 | 0.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | write_concurrent | 1 | 20k | concurrent | 369.9 | 24,427.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_cached | write_duplicate | 1 | 20k | concurrent | 83.5 | 12,467.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | mixed_read_write | 3 | 20k | batch64, concurrent, sequential | 1,171.5 | 23,842.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | read_batch_ordered64 | 3 | 20k | batch64, concurrent, sequential | 18,402.6 | 65.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | read_batch_unordered64 | 3 | 20k | batch64, concurrent, sequential | 86,240.7 | 139.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | read_concurrent_random | 3 | 20k | batch64, concurrent, sequential | 79,171.4 | 207.9 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | read_hot | 3 | 20k | batch64, concurrent, sequential | 18,847.5 | 65.4 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | read_random | 3 | 20k | batch64, concurrent, sequential | 18,651.7 | 66.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | read_sequential | 3 | 20k | batch64, concurrent, sequential | 18,853.3 | 66.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | write_batch64 | 1 | 20k | batch64 | 2,621.1 | 36,173.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | write_concurrent | 1 | 20k | concurrent | 362.9 | 28,192.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | write_duplicate | 3 | 20k | batch64, concurrent, sequential | 83.6 | 12,443.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator | write_sequential | 1 | 20k | sequential | 46.7 | 25,207.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | mixed_read_write | 1 | 20k | sequential | 2,245.5 | 18,285.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | read_batch_ordered64 | 1 | 20k | sequential | 18,419.7 | 69.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | read_batch_unordered64 | 1 | 20k | sequential | 67,793.1 | 188.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | read_concurrent_random | 1 | 20k | sequential | 73,739.6 | 254.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | read_hot | 1 | 20k | sequential | 18,887.7 | 64.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | read_random | 1 | 20k | sequential | 18,451.0 | 66.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | read_sequential | 1 | 20k | sequential | 18,885.6 | 64.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | write_duplicate | 1 | 20k | sequential | 851.3 | 1,225.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_coordinator_delay0 | write_sequential | 1 | 20k | sequential | 93.2 | 13,306.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | mixed_read_write | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 425.6 | 79,923.7 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | read_batch_ordered64 | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 31,699.6 | 34.9 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | read_batch_unordered64 | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 144,474.6 | 56.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | read_concurrent_random | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 164,542.0 | 143.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | read_hot | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 36,458.1 | 29.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | read_random | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 32,254.5 | 32.0 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | read_sequential | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 33,423.4 | 33.0 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | write_batch64 | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64 | 4,724.6 | 28,691.9 | per_item_batch_completion |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | write_concurrent | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent | 107.4 | 90,425.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | write_duplicate | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 44,221.2 | 25.1 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_engine | write_sequential | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | sequential | 105.4 | 12,613.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | mixed_read_write | 3 | 20k | batch64, concurrent, sequential | 689.2 | 161,702.8 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | read_batch_ordered64 | 3 | 20k | batch64, concurrent, sequential | 16,075.0 | 3,938.7 | per_item_batch_completion |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | read_batch_unordered64 | 3 | 20k | batch64, concurrent, sequential | 48,187.3 | 1,558.5 | per_item_batch_completion |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | read_concurrent_random | 3 | 20k | batch64, concurrent, sequential | 9,645.3 | 1,792.5 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | read_hot | 3 | 20k | batch64, concurrent, sequential | 7,275.3 | 161.3 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | read_random | 3 | 20k | batch64, concurrent, sequential | 7,250.8 | 164.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | read_sequential | 3 | 20k | batch64, concurrent, sequential | 7,204.6 | 163.7 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | write_batch64 | 1 | 20k | batch64 | 46.7 | 1,689,931.7 | per_item_batch_completion |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | write_concurrent | 1 | 20k | concurrent | 159.5 | 583,804.6 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | write_duplicate | 3 | 20k | batch64, concurrent, sequential | 81.7 | 12,725.2 | per_operation |
| benchmarks/results/pelagia-2026-10-26/summary.json | skerry_http | write_sequential | 1 | 20k | sequential | 46.5 | 24,421.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | mixed_read_write | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,229.8 | 5,573.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | read_batch_ordered64 | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,195.2 | 614.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | read_batch_unordered64 | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,735.3 | 4,243.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | read_concurrent_random | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 2,853.8 | 4,023.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | read_hot | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 14,833.6 | 398.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | read_random | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,074.5 | 639.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | read_sequential | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 3,059.5 | 637.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | write_concurrent | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent | 1,660.8 | 56,214.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | write_duplicate | 14 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent, sequential | 4,287.7 | 770.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_current | write_sequential | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | sequential | 956.7 | 1,540.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | mixed_read_write | 2 | 128k, 20k | sequential | 3,109.7 | 18,233.6 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | read_batch_ordered64 | 2 | 128k, 20k | sequential | 2,181.6 | 6,778.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | read_batch_unordered64 | 2 | 128k, 20k | sequential | 2,985.9 | 10,976.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | read_concurrent_random | 2 | 128k, 20k | sequential | 4,045.1 | 2,976.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | read_hot | 2 | 128k, 20k | sequential | 4,259.8 | 1,602.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | read_random | 2 | 128k, 20k | sequential | 2,103.4 | 6,766.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | read_sequential | 2 | 128k, 20k | sequential | 2,149.4 | 6,890.6 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | write_duplicate | 2 | 128k, 20k | sequential | 2,027.9 | 1,987.6 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_full_verify | write_sequential | 2 | 128k, 20k | sequential | 951.9 | 4,354.6 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | mixed_read_write | 2 | 1m, 20k | sequential | 1,737.6 | 6,408.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | read_batch_ordered64 | 2 | 1m, 20k | sequential | 3,728.9 | 430.0 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | read_batch_unordered64 | 2 | 1m, 20k | sequential | 3,634.3 | 3,163.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | read_concurrent_random | 2 | 1m, 20k | sequential | 2,935.2 | 3,867.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | read_hot | 2 | 1m, 20k | sequential | 11,410.0 | 526.0 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | read_random | 2 | 1m, 20k | sequential | 2,019.3 | 752.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | read_sequential | 2 | 1m, 20k | sequential | 2,037.6 | 722.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | write_duplicate | 2 | 1m, 20k | sequential | 1,484.1 | 1,544.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | kv2_stream | write_sequential | 2 | 1m, 20k | sequential | 568.9 | 2,521.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | mixed_read_write | 1 | 20k | concurrent | 2,089.4 | 14,550.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | read_batch_ordered64 | 1 | 20k | concurrent | 888,503.3 | 0.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | read_batch_unordered64 | 1 | 20k | concurrent | 543,392.0 | 15.7 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | read_concurrent_random | 1 | 20k | concurrent | 562,650.3 | 16.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | read_hot | 1 | 20k | concurrent | 1,359,442.4 | 0.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | read_random | 1 | 20k | concurrent | 840,981.4 | 0.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | read_sequential | 1 | 20k | concurrent | 1,319,516.2 | 0.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | write_concurrent | 1 | 20k | concurrent | 668.3 | 15,703.0 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_cached | write_duplicate | 1 | 20k | concurrent | 61,283.1 | 21.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | mixed_read_write | 3 | 20k | batch64, concurrent, sequential | 2,208.3 | 12,398.0 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | read_batch_ordered64 | 3 | 20k | batch64, concurrent, sequential | 22,945.4 | 50.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | read_batch_unordered64 | 3 | 20k | batch64, concurrent, sequential | 109,043.5 | 114.0 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | read_concurrent_random | 3 | 20k | batch64, concurrent, sequential | 119,862.5 | 122.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | read_hot | 3 | 20k | batch64, concurrent, sequential | 23,968.7 | 54.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | read_random | 3 | 20k | batch64, concurrent, sequential | 22,903.7 | 54.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | read_sequential | 3 | 20k | batch64, concurrent, sequential | 23,097.0 | 55.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | write_batch64 | 1 | 20k | batch64 | 4,431.6 | 29,192.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | write_concurrent | 1 | 20k | concurrent | 687.8 | 13,989.7 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | write_duplicate | 3 | 20k | batch64, concurrent, sequential | 61,513.4 | 22.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator | write_sequential | 1 | 20k | sequential | 86.1 | 13,911.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | mixed_read_write | 1 | 20k | sequential | 2,288.4 | 19,573.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | read_batch_ordered64 | 1 | 20k | sequential | 23,146.3 | 50.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | read_batch_unordered64 | 1 | 20k | sequential | 109,488.9 | 115.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | read_concurrent_random | 1 | 20k | sequential | 119,364.7 | 144.1 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | read_hot | 1 | 20k | sequential | 24,002.5 | 49.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | read_random | 1 | 20k | sequential | 23,294.6 | 53.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | read_sequential | 1 | 20k | sequential | 23,183.0 | 56.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | write_duplicate | 1 | 20k | sequential | 63,207.4 | 20.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_coordinator_delay0 | write_sequential | 1 | 20k | sequential | 95.9 | 13,382.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | mixed_read_write | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 459.9 | 74,324.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | read_batch_ordered64 | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 46,265.9 | 25.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | read_batch_unordered64 | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 213,353.7 | 49.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | read_concurrent_random | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 231,831.6 | 70.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | read_hot | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 51,010.3 | 22.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | read_random | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 47,902.0 | 24.0 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | read_sequential | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 48,075.1 | 23.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | write_batch64 | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64 | 5,209.0 | 25,128.2 | per_item_batch_completion |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | write_concurrent | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | concurrent | 109.9 | 90,169.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | write_duplicate | 21 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | batch64, concurrent, sequential | 61,604.0 | 19.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_engine | write_sequential | 7 | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed | sequential | 112.1 | 12,112.3 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | mixed_read_write | 3 | 20k | batch64, concurrent, sequential | 2,060.7 | 19,068.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | read_batch_ordered64 | 3 | 20k | batch64, concurrent, sequential | 20,878.1 | 3,060.6 | per_item_batch_completion |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | read_batch_unordered64 | 3 | 20k | batch64, concurrent, sequential | 64,841.9 | 1,123.5 | per_item_batch_completion |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | read_concurrent_random | 3 | 20k | batch64, concurrent, sequential | 12,180.3 | 1,440.8 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | read_hot | 3 | 20k | batch64, concurrent, sequential | 8,770.8 | 136.5 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | read_random | 3 | 20k | batch64, concurrent, sequential | 8,610.8 | 139.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | read_sequential | 3 | 20k | batch64, concurrent, sequential | 8,654.2 | 135.2 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | write_batch64 | 1 | 20k | batch64 | 4,533.9 | 28,754.8 | per_item_batch_completion |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | write_concurrent | 1 | 20k | concurrent | 668.8 | 14,269.4 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | write_duplicate | 3 | 20k | batch64, concurrent, sequential | 9,605.8 | 157.9 | per_operation |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | skerry_http | write_sequential | 1 | 20k | sequential | 88.5 | 14,545.3 | per_operation |

### Historical source row counts

| Source | Rows retained | Benchmark rows | Protocol phases | Verification scenarios | Foreground rows | HTTP cases | Streaming rows | Profiles | Phases | Datasets |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| docs/PERFORMANCE_UPGRADE_RESULTS.json | 68 | 0 | 0 | 0 | 0 | 60 | 8 | cache, quick | batch-put, hot-get, mixed, random-get, unique-put | 32768, 4096 |
| docs/HTTP_64_WORKER_RESULTS.json | 16 | 0 | 0 | 0 | 0 | 14 | 2 | cache, quick | batch-put, hot-get, mixed, random-get, unique-put | 32768, 4096 |
| docs/HTTP_CAPACITY_RESULTS.json | 20 | 0 | 0 | 0 | 0 | 20 | 0 | quick | batch-put, hot-get, mixed, random-get, unique-put | 32768, 4096 |
| docs/V2_PROTOCOL_BENCHMARK.json | 5 | 0 | 5 | 0 | 0 | 0 | 0 | V2 durability protocol comparison | batch64_write, duplicate_write, mixed_25_percent_write, random_read, single_write |  |
| docs/VERIFICATION_BENCHMARK.json | 3 | 0 | 0 | 3 | 0 | 0 | 0 | index-verification | deletion_churn, large_objects, small_objects |  |
| docs/VERIFICATION_FOREGROUND_BENCHMARK.json | 2 | 0 | 0 | 0 | 2 | 0 | 0 | foreground GET latency during verification | baseline_idle, while_verification_runs | 16777216 |
| benchmarks/results/pelagia-2026-10-26/summary.json | 423 | 423 | 0 | 0 | 0 | 0 | 0 | kv2_current, kv2_full_verify, kv2_stream, skerry_cached, skerry_coordinator, skerry_coordinator_delay0, skerry_engine, skerry_http | mixed_read_write, read_batch_ordered64, read_batch_unordered64, read_concurrent_random, read_hot, read_random, read_sequential, write_batch64, write_concurrent, write_duplicate, write_sequential | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed |
| benchmarks/results/pelagia-after-2026-10-26/summary.json | 423 | 423 | 0 | 0 | 0 | 0 | 0 | kv2_current, kv2_full_verify, kv2_stream, skerry_cached, skerry_coordinator, skerry_coordinator_delay0, skerry_engine, skerry_http | mixed_read_write, read_batch_ordered64, read_batch_unordered64, read_concurrent_random, read_hot, read_random, read_sequential, write_batch64, write_concurrent, write_duplicate, write_sequential | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed |
| benchmarks/results/fullsync-2026-10-26/summary.json | 54 | 54 | 0 | 0 | 0 | 0 | 0 | kv2_full_verify, kv2_macos_fullsync_verify, skerry_engine | mixed_read_write, read_batch_ordered64, read_batch_unordered64, read_concurrent_random, read_hot, read_random, read_sequential, write_concurrent, write_duplicate, write_sequential | 20k |
| benchmarks/results/kv2-after-lock-scope-2026-10-26/summary.json | 144 | 144 | 0 | 0 | 0 | 0 | 0 | kv2_current, kv2_stream | mixed_read_write, read_batch_ordered64, read_batch_unordered64, read_concurrent_random, read_hot, read_random, read_sequential, write_concurrent, write_duplicate, write_sequential | 128k, 1m, 20k, 32k, 5k, hotprefix, mixed |

### V2 protocol phases

These five before/after phases are a within-study durability protocol comparison. They do not produce cross-method deltas against HTTP or Pelagia results.

| Phase | Before ops/s | After ops/s | After / before |
| --- | ---: | ---: | ---: |
| single_write | 78.8 | 46.1 | 0.585 |
| duplicate_write | 5,273.6 | 3,951.6 | 0.749 |
| random_read | 8,137.9 | 9,014.8 | 1.108 |
| batch64_write | 5,067.8 | 3,077.5 | 0.607 |
| mixed_25_percent_write | 1,884.9 | 1,019.3 | 0.541 |

### Historical verification duration scenarios

These rows preserve within-study baseline/candidate verification wall durations; the source reports no operation-throughput rate for the scenario itself.

| Study | Scenario | Objects | Payload bytes | Deleted | Baseline median seconds | Candidate median seconds | Speedup | Baseline / candidate samples (s) |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| docs/VERIFICATION_BENCHMARK.json | small_objects | 2048 | 8388608 | 0 | 0.431 | 0.352 | 1.225× | 0.431, 0.445, 0.427 / 0.398, 0.349, 0.352 |
| docs/VERIFICATION_BENCHMARK.json | large_objects | 256 | 134217728 | 0 | 0.559 | 0.470 | 1.191× | 0.576, 0.559, 0.550 / 0.470, 0.484, 0.457 |
| docs/VERIFICATION_BENCHMARK.json | deletion_churn | 256 | 1048576 | 128 | 0.448 | 0.383 | 1.171× | 0.439, 0.448, 0.472 / 0.344, 0.383, 0.383 |

### Historical foreground GET latency during verification

These engine-level measurements report p95 (not p99) in milliseconds and requests/s for idle and overlapping verification conditions.

| Study | Condition | Requests | Overlapped requests | Requests/s | p50 ms | p95 ms | Total ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| docs/VERIFICATION_FOREGROUND_BENCHMARK.json | baseline_idle | 500 | 0 | 11,926.2 | 0.083 | 0.091 | 41.924 |
| docs/VERIFICATION_FOREGROUND_BENCHMARK.json | while_verification_runs | 500 | 500 | 12,137.2 | 0.081 | 0.088 | 41.196 |

## Historical comparison documents

The older KVStore2/Pelagia studies use different tools, payloads, durability contracts, and methods. Their source text is preserved below as context; no cross-method percent change is computed.

### benchmarks/COMPARISON.md

```text
# Pelagia KVStore2 versus SkerryKV — local comparison

At 20 KiB, Skerry direct warm random reads are 27.4× faster, and native bulk-64 writes reach 5,085 objects/s. Current KV2 single writes are 7.1× faster than Skerry single engine writes under their different synchronization contracts. Default Skerry HTTP writes remain limited by coordinator and request-path behavior; see the separate service rows and macOS control below.

Measured on 2026-10-26 using an Apple M3 Max, 16 logical CPUs, 128 GiB RAM and the same APFS SSD. Three trials, randomized serial case order; medians below. Fixtures are synthetic, deterministic, incompressible-like binary data. They fit in RAM. Timed reads follow explicit warmup passes: these are warm-cache, small-index results, not HDD/cold-cache/large-archive predictions. Disk free space was about 80 GiB on a 2 TB volume (roughly 96% full).

Pelagia uses the current source file and **packaged** BLAKE3 / prefix-length 2 / 64 GiB shard configuration, not an inspected live deployment configuration. Skerry direct-engine, coordinator, and loopback HTTP results are distinct. No production code or live store was modified. Initialization and fixture generation are excluded from phase throughput.

## Main result by payload size

| Payload | Objects/trial | KV2 single PUT/s | Skerry engine single PUT/s | Skerry engine bulk-64 PUT/s | KV2 random GET/s | Skerry engine random GET/s | Read ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| 5 KiB | 1024 | 737 | 114 | 5,444 | 1,758 | 139,215 | 79.2× |
| 20 KiB | 1024 | 741 | 104 | 5,085 | 1,767 | 48,388 | 27.4× |
| 32 KiB | 1024 | 771 | 110 | 5,129 | 1,756 | 31,938 | 18.2× |
| 128 KiB | 1024 | 658 | 111 | 3,384 | 1,678 | 8,688 | 5.2× |
| Mixed 5/20/32/128 KiB | 1024 | 722 | 105 | 4,725 | 1,755 | 22,845 | 13.0× |
| 1 MiB | 128 | 298 | 103 | 816 | 1,587 | 1,694 | 1.1× |

Single PUTs and native bulk-64 have different acknowledgment granularity; batching can amortize synchronization. KV2 has no native cross-object batch operation. MiB/s, sampled latency percentiles, and latency units are in the raw CSV/JSON.

## Access patterns at 20 KiB

| Pattern | KV2 ops/s | Skerry engine ops/s | Skerry/KV2 | KV2 p95 ms | Skerry p95 ms |
|---|---:|---:|---:|---:|---:|
| Single new writes | 741 | 104 | 0.14× | 5.638 | 10.573 |
| 8-client new writes | 1,555 | 102 | 0.07× | 6.417 | 81.009 |
| Duplicate hot writes | 1,622 | 69,044 | 42.58× | 1.701 | 0.016 |
| Sequential reads | 1,749 | 50,125 | 28.66× | 0.590 | 0.019 |
| Uniform random reads | 1,767 | 48,388 | 27.39× | 0.590 | 0.020 |
| 95% hot-set reads | 3,862 | 54,871 | 14.21× | 0.431 | 0.020 |
| 8-client random reads | 3,269 | 187,828 | 57.45× | 3.070 | 0.055 |
| Logical ordered batches | 1,809 | 48,439 | 26.77× | 0.592 | 0.022 |
| Logical unordered batches | 2,447 | 176,858 | 72.26× | 3.386 | 0.037 |
| 25% new writes / 75% reads | 2,404 | 422 | 0.18× | 5.992 | 77.748 |

Mixed actions are randomized independently of object size and distributed across workers; counts are fixed, closed-loop operations, not a constant-rate arrival process. The hot set is about 1% of objects. In-process read batches are bounded groups of individual API calls, not the HTTP binary batch protocol.

## Coordinator and HTTP overhead at 20 KiB

| Access layer | New single writes/s | New 8-client writes/s | Duplicate writes/s | Random reads/s | Random GET p95 ms |
|---|---:|---:|---:|---:|---:|
| KV2 direct Python | 741 | 1,555 | 1,622 | 1,767 | 0.590 |
| Skerry direct engine | 104 | 102 | 69,044 | 48,388 | 0.020 |
| Skerry coordinator, default 10 ms | 47 | 363 | 84 | 18,471 | 0.061 |
| Skerry coordinator, warm 256 MiB payload cache | — | 370 | 84 | 517,019 | 0.001 |
| Skerry loopback HTTP, default 10 ms | 46 | 160 | 82 | 7,129 | 0.152 |
| Skerry coordinator, delay=0 sensitivity | 93 | — | 851 | 18,451 | 0.061 |

The HTTP row includes network/client/parser/JSON costs and production default INFO logging; Pelagia was called in process, so it is not a matched comparison of two HTTP servers. The dataset payload size fits within the configured cache capacity; the profile includes explicit cache warmup. In-process cache throughput is not a network throughput claim.

## Bulk ingestion at 20 KiB

| Path | Logical objects/s | Actual operation |
|---|---:|---|
| KV2 | 741 | One put_store call per object |
| Skerry engine | 5,085 | Native 64-object atomic publication groups |
| Skerry coordinator | 2,621 | Up to 64 outstanding requests; bounded writer groups |
| Skerry HTTP batch-put | 47 | Binary batches of 64; current handler awaits each item |

Batch latency units differ: engine bulk latency is batch completion per member, HTTP streaming batch latency is item arrival from batch start, and individual calls have individual completion latency. Do not compare these percentile columns as one common latency metric. Units are preserved in raw output.

## Same-prefix contention

| Initial new writes, 8 workers | KV2 objects/s | Skerry engine objects/s |
|---|---:|---:|
| Uniform BLAKE3 prefixes, 1,024 objects | 1,555 | 102 |
| All original hashes prefix 00, 512 objects | 1,220 | 107 |

The forced-prefix fixture concerns original objects; mixed new objects are not prefix-filtered. These two cases have different counts, so initialization/growth effects also differ; rates are normalized but this is not a pure lock-only microbenchmark.

## Storage footprint after the mixed phase

| Dataset / implementation | Unique payload MiB | Final allocated file MiB | Allocated/payload | File count |
|---|---:|---:|---:|---:|
| 20 KiB / KV2 | 25.00 | 41.35 | 1.65× | 514 |
| 20 KiB / Skerry | 25.00 | 26.43 | 1.06× | 17 |
| 128 KiB / KV2 | 160.00 | 261.01 | 1.63× | 514 |
| 128 KiB / Skerry | 160.00 | 161.15 | 1.01× | 17 |
| 1 MiB / KV2 | 160.00 | 317.01 | 1.98× | 514 |
| 1 MiB / Skerry | 160.00 | 170.11 | 1.06× | 17 |

Final allocation is measured with filesystem stat blocks after the child closes. It includes store files/indexes and growth padding; it is not physical disk I/O or whole-filesystem metadata overhead.

## Durability and integrity controls

The unmodified KV2 profile uses SQLite WAL synchronous=NORMAL, Python os.fsync for blobs, and no payload hash recomputation on ordinary GET. Skerry synchronizes segment bytes and Fjall before success and verifies payload BLAKE3 on disk reads. These contracts are not equivalent. On this installed Rust/macOS target, sync_all uses F_FULLFSYNC; SQLite fullfsync defaults OFF and Python os.fsync is not that stronger call. A SQLite FULL-only sensitivity still does not align those macOS flush calls.

A separate, randomized control uses 256 objects × 20 KiB per trial, three trials, and the same fixtures across its three implementations. It runs after the primary suite, so cross-suite drift remains possible. The KV2 macOS-fullsync variant is an isolated benchmark adaptation: Python blob fsync calls use fcntl(F_FULLFSYNC), SQLite uses WAL/FULL with fullfsync=ON, and reads verify BLAKE3. This is a flush-strength control, not a claim of crash-recovery/directory-sync parity.

| Control / 256 objects | Single PUT/s | 8-client PUT/s | Random GET/s |
|---|---:|---:|---:|
| KV2 FULL + read hash, ordinary fsync | 986 | 1,642 | 2,268 |
| KV2 FULL + read hash + macOS fullsync | 36 | 54 | 2,198 |
| Skerry SyncAll + read hash | 109 | 110 | 77,054 |

Strengthening the flush calls reverses the single-write ranking within this control. This demonstrates that synchronization policy materially affects these measurements; it does not attribute every difference to one syscall or prove durability parity.

## Review findings

1. **Ordinary KV2 reads do not verify payload contents.** `kvstore2.py:432-442,1147-1161` checks metadata/header and length. A same-length payload corruption was returned by GET in a disposable probe; deep verification detected it. Skerry verifies BLAKE3 on disk reads. This is a reliability gap, not a performance optimization to copy.

2. **KV2 does not provide the same durable-ack contract.** Its index connection uses WAL/NORMAL at `kvstore2.py:1311-1316`; a system crash/power loss can lose recent committed mappings. Blob fsync before commit does not strengthen that index setting. No power-loss tests were run on either implementation.

3. **Per-call operations and periodic growth work deserve profiling.** Every GET opens a connection, sets WAL/synchronous/foreign-key pragmas, opens a blob, and parses a JSON record header. The connection context manager does not itself close it. Growth zero-fills and synchronizes on this host (no posix_fallocate), then payload writes synchronize again. See `kvstore2.py:907-923,1121-1145,1311-1317`.

4. **Stale-lock recovery relies on age, not owner liveness.** The probe obtained a second file lock after a short configured TTL while its owner was alive. Default TTL is one hour. This breaks file-lock exclusivity, but SQLite BEGIN IMMEDIATE independently serializes the examined append paths; the probe does not establish overlapping appends or corruption. See `kvstore2.py:94-146,302-305,982-985`.

5. **Skerry default service writes need tuning.** A sparse coordinator batch waits up to its configured window unless filled or interrupted. Native HTTP batch-put currently awaits each item, missing bulk grouping. HTTP admission reserves a maximum-object allowance: 256 MiB / 64 MiB gives four concurrent uploads despite small objects, so the 8-client benchmark includes retry costs. These are source-backed current behaviors, not limitations of Rust or Fjall.

## Multi-writer stress profile

`scripts/benchmark-http-capacity.py --profile writer-stress` adds duplicate and skewed write patterns across the configured client and writer-lane matrix. `duplicate-put` repeatedly sends one preloaded 4 KiB fixture and validates `duplicate=true`, so measured requests do not grow the physical object set. `skew-put` creates distinct 4 KiB payloads by deterministic nonce search until `hash[0] & 7 == 0`; hashes still span 32 virtual shards and route to physical lane zero for 1, 2, 4, or 8 lanes. Hash generation and rejection sampling consume client CPU. Localhost results include that work and same-host scheduling contention, so separate client and server hosts are preferable when attributing server capacity. Both patterns have the same payload size, fixture count, duration, and setup costs across lane variants. The normal `quick` profile is unchanged.

## Scope and next steps

Read gains here concern warm, small indexes; bulk gains include batching and synchronization amortization. Do not infer disk-cold, HDD, very large index, sustained compaction, crash-recovery speed, Windows, or Linux performance from this Mac. The large-object corner has 128 operations/trial, so p99 tails are especially noisy. Synthetic size distributions are assumptions, not a measured Pelagia traffic distribution.

Prioritize Skerry bounded HTTP batch pipelining and upload admission based on known content length, plus a durable-safe duplicate fast path. Preserve the acknowledgment contract. Independently add ordinary-read integrity verification and explicit durability/connection-lifetime policies to KV2 before treating it as an archive reliability baseline. Re-run on the intended Linux/Windows storage and a representative working set before migration. KV2 blobs are not directly importable Skerry segments; migration must validate existing hashes and stream/reinsert objects, with reference mapping if source hashes are SHA-256.

## Evidence and reproduction

Primary audit: {'completed_cases': 141, 'validated_phases': 1269, 'validated_operations': 1085184, 'payload_or_operation_errors': 0}.
Control audit: {'completed_cases': 18, 'validated_phases': 162, 'validated_operations': 41472, 'payload_or_operation_errors': 0}.

- [Harness instructions](README.md)
- [Primary raw results](results/pelagia-2026-10-26/summary.json)
- [Primary measurements CSV](results/pelagia-2026-10-26/measurements.csv)
- [Supplemental control](results/fullsync-2026-10-26/summary.json)
- [Review probe evidence](review-evidence.json)
- [Flush source evidence](sync-semantics.json)

References: [SQLite synchronous/fullfsync](https://www.sqlite.org/pragma.html#pragma_synchronous), [Python connection context manager](https://docs.python.org/3.12/library/sqlite3.html#how-to-use-the-connection-context-manager), [Rust Apple sync implementation](https://doc.rust-lang.org/src/std/sys/fs/unix.rs.html).
```

### benchmarks/IMPROVEMENTS.md

```text
# SkerryKV and Pelagia performance improvements

At 20 KiB, HTTP bulk ingestion increased from 47 to 4,534 objects/s (97.1×), and HTTP mixed throughput from 665 to 1,992 ops/s (3.0×). Final KV2 results below include the narrower-lock refinement; rows report regressions as well as gains.

Measured on Apple M3 Max (macOS-26.6.2-arm64-arm-64bit) with 137,438,953,472 bytes RAM and the same APFS SSD. The runtime versions match: Python `3.12.2 | packaged by conda-forge | (main, Feb 16 2024, 20:54:21) [Clang 16.0.6 ]` and `rustc 1.94.1 (e408947bf 2026-03-25)`. The original and main updated suites use identical fixture manifests and seeds, 141 cases each, three randomized serial trials, and report trial medians. A final 48-case KV2-only suite measures the narrower cache lock scope with the same fixture manifests, three trials, and runtime. All KV2 current rows below use that final suite; all Skerry rows use the main updated suite. The suites ran sequentially, so environmental drift remains possible.

Reads use an explicit warmup pass. These results describe warm-cache workloads and a small in-memory index. They do not establish Linux HDD cold-cache performance or power-loss behavior. The service batch delay changed from 10 ms before to 1 ms after; this is a deliberate configuration and code change measured together, so its effects cannot be attributed separately.

The updated current KV2 profile adds a 16-connection LRU per thread across 256 hash prefixes, with explicit shutdown cleanup and pruning of finished-thread caches. SQLite open, PRAGMAs, and close/checkpoint work execute outside the cache-wide condition; active leases keep shutdown waiting until this work finishes. It keeps WAL/NORMAL and the existing ordinary-read integrity behavior. The cache has low potential hit rate for uniform access across all prefixes. The full-verify benchmark override bypasses the cache and is not evidence of a production KV2 improvement.

## KV2 current profile by payload size

Random read, hot-set read, and mixed workload rates (operations/s):

| Payload | Random before | Random after | Hot before | Hot after | Mixed before | Mixed after |
|---|---:|---:|---:|---:|---:|---:|
| 5 KiB | 1,758 | 2,219 | 3,989 | 15,108 | 2,559 | 3,009 |
| 20 KiB | 1,767 | 2,226 | 3,862 | 13,585 | 2,404 | 2,629 |
| 32 KiB | 1,756 | 2,224 | 3,510 | 13,748 | 2,446 | 2,572 |
| 128 KiB | 1,678 | 2,160 | 3,528 | 12,623 | 2,360 | 2,897 |
| 1 MiB | 1,587 | 1,911 | 3,057 | 8,950 | 1,290 | 1,354 |
| Mixed sizes | 1,755 | 2,227 | 3,863 | 14,153 | 2,638 | 2,926 |

## 20 KiB workloads

### KV2 current

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Single write | 741 | 900 | 1.21× | 5.638 | 1.477 |
| Concurrent writes | 1,555 | 2,005 | 1.29× | 6.417 | 5.555 |
| Random reads | 1,767 | 2,226 | 1.26× | 0.590 | 0.599 |
| Hot-set reads | 3,862 | 13,585 | 3.52× | 0.431 | 0.380 |
| Concurrent random reads | 3,269 | 3,932 | 1.20× | 3.070 | 2.954 |
| Mixed 25% writes / 75% reads | 2,404 | 2,629 | 1.09× | 5.992 | 5.286 |

### Skerry coordinator and HTTP

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Coordinator single write | 47 | 86 | 1.84× | 22.502 | 12.396 |
| Coordinator concurrent write | 363 | 688 | 1.90× | 23.308 | 12.411 |
| Coordinator bulk-64 | 2,621 | 4,432 | 1.69× | 36.164 | 28.620 |
| Coordinator duplicate write | 84 | 62,658 | 749.16× | 12.193 | 0.018 |
| Coordinator random read | 18,471 | 22,904 | 1.24× | 0.061 | 0.049 |
| Coordinator mixed | 1,130 | 2,201 | 1.95× | 22.402 | 11.842 |
| HTTP single write | 46 | 88 | 1.90× | 22.898 | 12.407 |
| HTTP concurrent write | 160 | 669 | 4.19× | 23.474 | 13.197 |
| HTTP bulk-64 | 47 | 4,534 | 97.09× | 1329.735 | 28.650 |
| HTTP duplicate write | 82 | 9,557 | 116.65× | 12.483 | 0.147 |
| HTTP random read | 7,129 | 8,702 | 1.22× | 0.152 | 0.127 |
| HTTP mixed | 665 | 1,992 | 3.00× | 22.501 | 16.604 |

### Coordinator delay=0 sensitivity

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Coordinator delay=0 single write | 93 | 96 | 1.03× | 11.405 | 11.177 |
| Coordinator delay=0 random read | 18,451 | 23,295 | 1.26× | 0.061 | 0.047 |
| Coordinator delay=0 mixed | 2,246 | 2,288 | 1.02× | 11.567 | 10.832 |

### Direct-engine control (unchanged operation paths)

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Direct engine single writes | 104 | 116 | 1.12× | 10.573 | 9.914 |
| Direct engine random reads | 48,388 | 72,249 | 1.49× | 0.020 | 0.015 |
| Direct engine mixed | 422 | 465 | 1.10× | 77.748 | 68.917 |

These direct-engine operations do not use the coordinator fast path or HTTP changes. Changes in these control rates show that before/after ratios also contain environmental or build variation; they are not isolated causal estimates.

### KV2 cache lock-scope follow-up

| 20 KiB operation | Initial pooled version ops/s | Final narrower-lock version ops/s |
|---|---:|---:|
| Eight-client random reads | 2,872 | 3,932 |
| 25% new writes / 75% reads | 2,173 | 2,629 |

## Changes and interpretation

The after implementation reduces the default service batch delay to 1 ms, pipelines up to 64 HTTP batch writes, reserves actual known upload sizes, and lets healthy, committed duplicates bypass the writer queue. Existing config files keep their values; set `writes.batch_max_delay_ms = 1` explicitly to adopt the tested setting. The suite reports their combined outcome, including single and concurrent writes, duplicate writes, bulk-64, random reads, and mixed traffic. Per-row ratios are computed from the audited suite medians. Individual and bulk write latency use different units: the HTTP bulk percentile measures each item’s arrival from batch start, while individual calls measure call-to-return latency.

The HTTP pipeline now handles bounded pending requests with unknown outcomes explicitly. After a storage failure, decoding stops and ordered outcomes are returned for already-queued items when the response remains usable. A missing result leaves its outcome unknown; content-addressed PUT can be safely retried. Benchmark cases require successful replies and exact content validation, and have zero reported operation errors. Review the implementation’s documented retry and error semantics alongside throughput before relying on pipeline results.

The KV2 comparisons use the production current profile with the new connection lifecycle. Because each thread retains only 16 shard connections across 256 possible prefixes, uniform random reads have low connection-cache hit potential. Full-verify bypasses that cache and changes integrity work, so it is intentionally excluded from production improvement claims.

## Method and limits

The harness uses the same deterministic fixture seeds and dataset manifests in both suites. Three randomized serial trials were run on the same hardware/runtime; summary values are medians. Reads are warm after a full read pass, and the dataset fits in RAM. The 1 ms service delay and code path changed together, preventing isolated attribution to delay tuning. This benchmark does not measure Linux HDD cold-cache workloads, power-loss outcomes, or durability parity. Throughput must be interpreted with each implementation’s acknowledgment and synchronization behavior.

## Evidence

Baseline audit: `{'completed_cases': 141, 'validated_phases': 1269, 'validated_operations': 1085184, 'errors': 0}`. Main updated audit: `{'completed_cases': 141, 'validated_phases': 1269, 'validated_operations': 1085184, 'errors': 0}`. Final KV2 audit: `{'completed_cases': 48, 'validated_phases': 432, 'validated_operations': 342144, 'errors': 0}`.

- [Baseline summary](results/pelagia-2026-10-26/summary.json) · [baseline CSV](results/pelagia-2026-10-26/measurements.csv)
- [After summary](results/pelagia-after-2026-10-26/summary.json) · [after CSV](results/pelagia-after-2026-10-26/measurements.csv)
- [Final KV2 summary](results/kv2-after-lock-scope-2026-10-26/summary.json) · [final KV2 CSV](results/kv2-after-lock-scope-2026-10-26/measurements.csv)
- [Benchmark harness instructions](README.md) · [comparison harness](compare_pelagia.py)


Timed HTTP phases recorded 5,226 → 0 HTTP 503 retries and 2,622 → 0 client transport retries. Retry costs are included in the measured operations.
```

## Streaming capacity rows

These large-object HTTP runs retain their original MiB/s units and are presented separately from object operations/s. Historical aggregate inputs and repeatable `--streaming` files/directories are preserved in JSON.

| Study | Origin | Mode | File MiB | Clients | Lanes | Throughput MiB/s | p99 ms | Rejected / unknown |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | get | 16 | 16 | None | 6,104.0 | 49.18 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | get | 16 | 32 | None | 6,096.6 | 91.43 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | get | 16 | 4 | None | 5,649.5 | 18.73 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | get | 16 | 8 | None | 6,076.4 | 29.75 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | put | 16 | 16 | None | 299.4 | 1,379.40 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | put | 16 | 32 | None | 337.6 | 1,678.18 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | put | 16 | 4 | None | 287.0 | 767.28 | 0 / 0 |
| docs/PERFORMANCE_UPGRADE_RESULTS.json:streaming | historical | put | 16 | 8 | None | 355.7 | 421.21 | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:streaming | historical | get | 16 | 64 | None | 6,009.9 | 188.96 | 0 / 0 |
| docs/HTTP_64_WORKER_RESULTS.json:streaming | historical | put | 16 | 64 | None | 290.9 | 3,750.30 | 0 / 0 |
| target/multi-writer-stream-0.3/get-c16-w1.json:streaming | candidate | get | 16 | 16 | 1 | 6,079.3 | 48.47 | 0 / 0 |
| target/multi-writer-stream-0.3/get-c32-w1.json:streaming | candidate | get | 16 | 32 | 1 | 6,065.8 | 92.86 | 0 / 0 |
| target/multi-writer-stream-0.3/get-c4-w1.json:streaming | candidate | get | 16 | 4 | 1 | 5,648.5 | 19.02 | 0 / 0 |
| target/multi-writer-stream-0.3/get-c64-w1.json:streaming | candidate | get | 16 | 64 | 1 | 6,030.4 | 178.90 | 0 / 0 |
| target/multi-writer-stream-0.3/get-c8-w1.json:streaming | candidate | get | 16 | 8 | 1 | 6,114.5 | 27.94 | 0 / 0 |
| target/multi-writer-stream-0.3/put-c16-w1.json:streaming | candidate | put | 16 | 16 | 1 | 349.1 | 799.98 | 0 / 0 |
| target/multi-writer-stream-0.3/put-c32-w1.json:streaming | candidate | put | 16 | 32 | 1 | 350.1 | 1,551.69 | 0 / 0 |
| target/multi-writer-stream-0.3/put-c4-w1.json:streaming | candidate | put | 16 | 4 | 1 | 333.6 | 236.79 | 0 / 0 |
| target/multi-writer-stream-0.3/put-c64-w1.json:streaming | candidate | put | 16 | 64 | 1 | 336.8 | 3,117.62 | 0 / 0 |
| target/multi-writer-stream-0.3/put-c8-w1.json:streaming | candidate | put | 16 | 8 | 1 | 352.2 | 416.55 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c16-w1.json:streaming | candidate | get | 16 | 16 | 1 | 6,067.4 | 51.16 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c16-w2.json:streaming | candidate | get | 16 | 16 | 2 | 6,011.3 | 51.48 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c16-w4.json:streaming | candidate | get | 16 | 16 | 4 | 6,059.7 | 51.09 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c16-w8.json:streaming | candidate | get | 16 | 16 | 8 | 6,039.2 | 53.15 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c32-w1.json:streaming | candidate | get | 16 | 32 | 1 | 6,052.3 | 94.39 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c32-w2.json:streaming | candidate | get | 16 | 32 | 2 | 6,017.8 | 97.16 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c32-w4.json:streaming | candidate | get | 16 | 32 | 4 | 6,021.3 | 94.33 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c32-w8.json:streaming | candidate | get | 16 | 32 | 8 | 6,016.3 | 97.78 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c4-w1.json:streaming | candidate | get | 16 | 4 | 1 | 5,642.5 | 14.74 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c4-w2.json:streaming | candidate | get | 16 | 4 | 2 | 5,661.1 | 15.64 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c4-w4.json:streaming | candidate | get | 16 | 4 | 4 | 5,653.2 | 15.03 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c4-w8.json:streaming | candidate | get | 16 | 4 | 8 | 5,608.6 | 19.80 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c64-w1.json:streaming | candidate | get | 16 | 64 | 1 | 6,056.4 | 179.59 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c64-w2.json:streaming | candidate | get | 16 | 64 | 2 | 6,050.3 | 178.21 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c64-w4.json:streaming | candidate | get | 16 | 64 | 4 | 6,056.0 | 177.63 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c64-w8.json:streaming | candidate | get | 16 | 64 | 8 | 6,056.1 | 175.81 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c8-w1.json:streaming | candidate | get | 16 | 8 | 1 | 6,095.2 | 26.90 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c8-w2.json:streaming | candidate | get | 16 | 8 | 2 | 6,081.2 | 27.22 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c8-w4.json:streaming | candidate | get | 16 | 8 | 4 | 6,042.6 | 29.50 | 0 / 0 |
| target/multi-writer-stream-0.4/get-c8-w8.json:streaming | candidate | get | 16 | 8 | 8 | 6,064.3 | 28.43 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c16-w1.json:streaming | candidate | put | 16 | 16 | 1 | 333.0 | 954.85 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c16-w2.json:streaming | candidate | put | 16 | 16 | 2 | 347.2 | 776.82 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c16-w4.json:streaming | candidate | put | 16 | 16 | 4 | 330.2 | 920.56 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c16-w8.json:streaming | candidate | put | 16 | 16 | 8 | 333.2 | 811.28 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c32-w1.json:streaming | candidate | put | 16 | 32 | 1 | 339.6 | 1,532.92 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c32-w2.json:streaming | candidate | put | 16 | 32 | 2 | 315.4 | 1,742.52 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c32-w4.json:streaming | candidate | put | 16 | 32 | 4 | 335.8 | 1,586.69 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c32-w8.json:streaming | candidate | put | 16 | 32 | 8 | 327.9 | 1,722.66 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c4-w1.json:streaming | candidate | put | 16 | 4 | 1 | 359.9 | 219.80 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c4-w2.json:streaming | candidate | put | 16 | 4 | 2 | 346.6 | 220.76 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c4-w4.json:streaming | candidate | put | 16 | 4 | 4 | 337.1 | 239.22 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c4-w8.json:streaming | candidate | put | 16 | 4 | 8 | 324.6 | 277.31 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c64-w1.json:streaming | candidate | put | 16 | 64 | 1 | 346.1 | 3,004.32 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c64-w2.json:streaming | candidate | put | 16 | 64 | 2 | 344.7 | 3,017.67 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c64-w4.json:streaming | candidate | put | 16 | 64 | 4 | 307.1 | 3,374.39 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c64-w8.json:streaming | candidate | put | 16 | 64 | 8 | 332.4 | 3,220.99 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c8-w1.json:streaming | candidate | put | 16 | 8 | 1 | 347.8 | 425.57 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c8-w2.json:streaming | candidate | put | 16 | 8 | 2 | 346.3 | 403.22 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c8-w4.json:streaming | candidate | put | 16 | 8 | 4 | 349.6 | 407.76 | 0 / 0 |
| target/multi-writer-stream-0.4/put-c8-w8.json:streaming | candidate | put | 16 | 8 | 8 | 330.9 | 484.43 | 0 / 0 |
Full source metadata, fingerprints, snapshots, and rows are in the adjacent JSON report.
