###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import csv
import json
import logging
import os
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

LOGGER = logging.getLogger(__name__)

DATADIR = os.getenv("WIS2BOX_HOST_DATADIR")
THISDIR = os.path.dirname(os.path.realpath(__file__))

es_api = os.getenv("WIS2BOX_API_BACKEND_URL")
es_index = "stations"
station_file = Path(DATADIR) / "metadata" / "station" / "station_list.csv"


def apply_mapping(value, mapping):
    return mapping.get(value, value)  # noqa use existing value as default in case no match found


def apply_mapping_elastic(records, codelists, code_maps):
    updates = []
    for idx in range(len(records)):
        record = records[idx]['_source']
        # iterate over code lists and map entries
        for codelist in codelists:
            if codelist in record['properties']:
                record['properties'][codelist] = code_maps[codelist].get(
                    record['properties'][codelist],
                    record['properties'][codelist]  # noqa use existing value as default in case no match found
                )
            else:
                print(f"No matching element found {codelist}")
        # now update record for ES
        updates.append({
            "_op_type": "update",
            "_index": records[idx].get('_index'),
            "_id": records[idx].get('_id'),
            "doc": record
        })

    return updates


def migrate(dryrun: bool = False):
    # first load code lists / mappings
    code_maps = {}
    codelists = ('facility_type', 'territory_name', 'wmo_region')
    for codelist in codelists:
        p = Path(THISDIR)
        mapping_file = p / f"{codelist}.json"
        with open(mapping_file) as fh:
            code_maps[codelist] = json.load(fh)

    # First migrate / update data in station list CSV file
    # list to store stations
    stations = []
    # open station file and map
    with open(station_file, 'r') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            for codelist in codelists:
                if codelist in row:
                    row[codelist] = apply_mapping(row.get(codelist),
                                                  code_maps.get(codelist))
                else:
                    pass
            stations.append(row)

    if dryrun:
        print(','.join(map(str, stations[0].keys())))
        for station in stations:
            print(','.join(map(str, station.values())))
    else:
        # now write data to file
        with open(f"{station_file}.v1.0b7", 'w') as fh:
            columns = list(stations[0].keys())
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            for station in stations:
                writer.writerow(station)

    # now migrate ES data
    # Get elastic search connection
    es = Elasticsearch(es_api)
    more_data = True  # flag to keep looping until all data processed
    batch_size = 100  # process in batch sizes of 100
    cursor = 0  # cursor to keep track of position

    # now loop until all data processed
    while more_data:
        res = es.search(index=es_index,
                        query={'match_all': {}},
                        size=batch_size,
                        from_=cursor)

        nhits = len(res['hits']['hits'])
        cursor += batch_size
        if nhits < batch_size:
            more_data = False
        stations = res['hits']['hits']
        updates = apply_mapping_elastic(stations, codelists, code_maps)
        if dryrun:
            print(updates)
        else:
            bulk(es, updates)
