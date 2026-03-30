TPC-C 

TPC-C is an On-Line Transaction Processing Benchmark by the Transaction Processing Performance Council (See https://www.tpc.org/tpcc).


The TPC-C Schema (partial): A wholesale supplier operates out of several warehouses.  The warehouses maintain stocks for the items sold by the company. We record the quantity in stock for each item available in each warehouse. Warehouses have a unique identifier, a name and a location defined by a street, city and country. items have a unique identifier, a unique image identifier, a name and a price. 

Files:

TPCCSchema.sql creates the tables with their constraints,
TPCCItems.sql populates the items table,
TPCCWarehouses.sql populates the warehouses table,
TPCCStocks.sql populates the stock table,
TPCCClean.sql drops all the tables.
