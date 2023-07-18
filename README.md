# arrow-hash

A hacked-out 2-hour prototype to investigate hash tables over arrow arrays.

Callled from Python, lookups are about 6us.  Table construction is about 250ns per array element (250ms for 1m elements)

In comparison, for a plain Python dict, lookups are about 1us, and table construction is about 600ns per element.

```
In [1]: import numpy as np; import pyarrow as pa; import arrow_hash

In [2]: data = pa.array(np.random.choice(np.arange(1000), 100000))

In [3]: %timeit arrow_hash.ArrowInt64Index(data)
27.4 ms ± 147 µs per loop (mean ± std. dev. of 7 runs, 10 loops each)

In [4]: data = pa.array(np.random.choice(np.arange(100), 100000))

In [5]: %timeit arrow_hash.ArrowInt64Index(data)
25.7 ms ± 73.6 µs per loop (mean ± std. dev. of 7 runs, 10 loops each)

In [6]: data = pa.array(np.random.choice(np.arange(100), 100))

In [7]: %timeit arrow_hash.ArrowInt64Index(data)
38.9 µs ± 65.7 ns per loop (mean ± std. dev. of 7 runs, 10,000 loops each)

In [8]: data = pa.array(np.random.choice(np.arange(100), 200))

In [9]: %timeit arrow_hash.ArrowInt64Index(data)
70.9 µs ± 127 ns per loop (mean ± std. dev. of 7 runs, 10,000 loops each)

In [10]: 
```
