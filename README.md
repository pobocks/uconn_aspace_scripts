# UConn ASpace Scripts

This is a repository for scripts to support a data remediation project for the University of Connecticut.

Currently the following scripts are included:

## report.py

Script for running various SQL-based reports. Output is currently as tab-separated values, specifically matching the [excel-tab dialect](https://docs.python.org/3/library/csv.html#csv.excel_tab) of Python's csv module.

### Usage

```
usage: report.py [-h] [--reports [{unattached_containers} ...]]

Script for running various reports, outputting tsv

options:
  -h, --help            show this help message and exit
  --reports [{unattached_containers} ...]
                        Which reports to run, leave empty for all
```
