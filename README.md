# Data Test A - Dependencies Orchestration

This is a working solution for Sephora SEA data test parts A1, A2 and A3. Part B is available in the following repo: [data-test-sde-alan-B](https://github.com/emailayuen/data-test-sde-alan-B).

## Approach

A1: Create a `master tree` structure that captures all tables and their dependencies by parsing the SQL scripts and extrapolating the required info.
 ```
 Example output:
  
  {
    "final.products": [
      "tmp.products",
      "tmp.variants"
    ],
    "tmp.inventory_items": [
      "raw.inventory_items"
    ],
    "tmp.item_purchase_prices": [
      "raw.purchase_line_items",
      "raw.purchase_items"
    ]
}
```
A2: Work out the correct sequence in which these tables have to be run, by representing the `master tree` using a `Graph` data structure (nodes and edges), and writing an algorithm that works out the correct order.

A3: Finally, we want to take advantage of parallelism by determining which `nodes(tables)` can be run at the same time, rather than running everything sequentially. To solve this problem, an attribute called `level` is added to each `node(table)` object within the `master tree`. All `nodes(tables)` belonging to the same `level` can be run in parallel, but each `level` cannot begin running until the previous level has completed. 
 ```
 
 ```

## Deploy and Run Locally

### Prerequisites

* [Python 3.7](https://www.python.org/downloads/) (3.6 should suffice but untested)

### Setup

To run the program...

  1. Pull down the source code to your local machine and navigate to the main project folder.
  
  ```
  Example:
  
  cd C:\git\data-test-sde-alan-A
  ```
  
  2. To run, simply pass the main program as an argument to the Python interpreter.

  ```
  Example:
  
  C:\git\data-test-sde-alan-A>python main.py
  ```
