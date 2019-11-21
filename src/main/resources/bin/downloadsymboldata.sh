#!/bin/bash

RESOURCE_DIR=$(dirname $(dirname $(readlink -fm "$0")))

# Get unique API key from https://www.alphavantage.co/support/#api-key
apikey=$1
symbol=$2

# Downloading the list of symbols on NASDAQ:
wget --method GET \
  --header 'cache-control: no-cache' \
  --output-document \
  - "https://www.alphavantage.co/query?apikey=$apikey&function=TIME_SERIES_DAILY&symbol=$symbol&datatype=csv&outputsize=full" > "$RESOURCE_DIR"/data/daily_"$symbol".csv
