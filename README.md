# arrow-hash

A hacked-out 2-hour prototype to investigate hash tables over arrow arrays.

| operation              | python dict | arrow-hash |
|------------------------|-------------|------------|
| index 1 million points | 550ms       | 40ms       |
| lookup                 | 1us         | 1.5us      |


