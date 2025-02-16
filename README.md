# UConn ASpace Scripts

This is a repository for scripts to support a data remediation project for the University of Connecticut.

Several scripts are included, and are intended to be run in order to address problems in the database:

- `report.py`
- `fix_indicators.py`
- `delete_unattached.py`
- `box_folder_decombinator.py`

## `report.py`

Script for running various SQL-based reports. Output is currently as tab-separated values, specifically matching the [excel-tab dialect](https://docs.python.org/3/library/csv.html#csv.excel_tab) of Python's csv module.

### Usage

```
usage: report.py [-h]
                 [--reports [{1a__unattached_containers,1b__01__bad_indicators,1b__box_folder_combo_containers,1c__file_or_item_without_container,1d__digitized_containers,1e_top_containers_with_duplicate_indicators,1g_invalid_barcodes} ...]]
                 [--dev-fields] [--dbname DBNAME] [--dbuser DBUSER]

Script for running various reports, outputting tsv

options:
  -h, --help            show this help message and exit
  --reports [{1a__unattached_containers,1b__01__bad_indicators,1b__box_folder_combo_containers,1c__file_or_item_without_container,1d__digitized_containers,1e_top_containers_with_duplicate_indicators,1g_invalid_barcodes} ...]
                        Which reports to run, leave empty for all
  --dev-fields, -d      Include fields used for development that aren't useful
                        to staff
  --dbname DBNAME       override name of database to query
  --dbuser DBUSER       override db username
```

## `fix_indicators.py`

Script that corrects several issues with indicators, for example trailing spaces and punctuation characters.

### Usage

```
usage: fix_indicators.py [-h] [--host HOST] [--port PORT]

options:
  -h, --help            show this help message and exit
  --host HOST, -H HOST  mysql host
  --port PORT, -p PORT  mysql port
```

## `delete_unattached.py`

Script that deletes containers that have no contents.

### Usage

```
usage: delete_unattached.py [-h] [--dry-run] report

positional arguments:
  report         Report file to work from

options:
  -h, --help     show this help message and exit
  --dry-run, -n  make no changes but print what would happen
```

## `box_folder_decombinator.py`

Script that groups containers matching the format `box_num:folder_num`, groups them by box number, and either finds or creates the top container with that box number, and turns the grouped containers into sub-containers attached to the top container.

### Usage

```
usage: box_folder_decombinator.py [-h] [--dry-run] [--log-config LOG_CONFIG]
                                  report

positional arguments:
  report                Report file to work from

options:
  -h, --help            show this help message and exit
  --dry-run, -n         make no changes but print what would happen
  --log-config LOG_CONFIG
                        log constant from asnake.logging
```
