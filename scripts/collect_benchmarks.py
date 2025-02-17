from os import listdir
import csv
import re
import sys
import os
from os.path import isfile


def parse_filename(filename):
    # Define the regular expression patterns for the two formats
    # pattern1 = re.compile(r'^(?P<filename>[^_]+)_split_(?P<r>\d+)\.fa\.(?P<approach>[^.]+)\.csv$')
    # pattern2 = re.compile(r'^(?P<filename>[^.]+)\.fa\.(?P<approach>[^.]+)\.csv$')

    pattern_partdna = re.compile(r'^(?P<filename>[a-zA-Z0-9]+)\.(?P<extension>fa|fa\.gz|fq|fq\.gz|owpl)_(?P<r>\d+)\.partdna\.csv$')
    match = pattern_partdna.match(filename)
    if match:
        return 'partdna', match.group('filename'), match.group('r'), match.group(
            'extension')  # r is none if not included

    pattern = re.compile(r'^(?P<filename>[a-zA-Z0-9]+)(?:_split_(?P<r>\d+))?\.(?P<extension>fa|fa\.gz|fq|fq\.gz|owpl)\.(?P<approach>[a-zA-Z0-9_]+)\.csv$')
    match = pattern.match(filename)
    if match:
        return match.group('approach'), match.group('filename'), match.group('r'), match.group('extension')  # r is none if not included

    # If neither pattern matches, raise an error
    raise ValueError(f"Filename '{filename}' does not match expected patterns")


def get_success_indicator(filename):
    if 'partdna' in filename:
        return '1'
    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            for line in f:
                return line[0]
    else:
        print(f'indicator "{filename}" is missing. I assume failure.')
        return '0'
        # raise FileNotFoundError(f'File indicators/{filename}.{file_extension}.{approach} not found.')


def combine(data_sets, approaches, DATA_TYPE, out_file):
    with open(out_file, "w") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(['algorithm', 'dataset', 'r', 'successful', 's', 'h:m:s', 'max_rss', 'max_vms', 'max_uss', 'max_pss', 'io_in', 'io_out', 'mean_load', 'cpu_time'])
        for data_set in data_sets:
            for approach in approaches:
                bench = f'bench/{data_set}.{DATA_TYPE[approach]}.{approach}.csv'
                indicator = f'indicators/{data_set}.{DATA_TYPE[approach]}.{approach}'
                if not isfile(bench):
                    continue
                with open(bench, 'r') as g:
                    reader = csv.reader(g, delimiter="\t")
                    next(reader)  # Headers line
                    success = get_success_indicator(indicator)
                    writer.writerow([approach, data_set, '0', success] + next(reader))
