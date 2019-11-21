#!/bin/bash

RESOURCE_DIR=$(dirname $(dirname $(readlink -fm $0)))

# Downloading the list of symbols on NASDAQ:
wget --method GET \
  --header 'cache-control: no-cache' \
  --output-document \
  - 'https://old.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download' > $RESOURCE_DIR/data/companylist.csv
