# disp-scrip
Script with Polars to parse customer-store interactions.

This repo is primarily to show a real and currently in use script using polars within the command line interface.
While these transformations can be done in pandas, the source files contain millions of rows and take a considerable time to process with pandas.
Polars ability to lazy read a csv and take advantage of multithreading drastically cuts down on the time it takes to process a file.

All references to proprietary data have been removed.
