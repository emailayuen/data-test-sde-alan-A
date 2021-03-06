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

A3: With the correct running order in place, we now want to take advantage of parallelism by determining which `nodes(tables)` can be run at the same time, rather than running everything sequentially. To solve this problem, an attribute called `level` is added to each `node` within the `master tree`. All `nodes` belonging to the same `level` can be run in parallel, but each `level` cannot begin running until the previous level has completed. 
 ```
 Example:
 
 Level 1 (run below in parallel):
  - tmp.product_images
  - tmp.inventory_items
  - tmp.purchase_prices
  - tmp.product_categories
  - tmp.variant_images
  
  Level 2 (...after last table in Level 1 completes, run below in parallel):
  - tmp.products
  - tmp.variants
  
  Level 3 (...after last table in Level 2 completes, run below in parallel):
  - final.products
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
