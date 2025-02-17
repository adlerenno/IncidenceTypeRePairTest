import os
from itertools import chain

MAX_MAIN_MEMORY = 128
NUMBER_OF_PROCESSORS = 32

# Avoid renaming these, because they are mostly not referenced but hard coded.
DIR = "./"
INPUT = './input/'
TEMP = './tmp/'
OUTPUT = './data/'
INDICATORS = './indicators/'
BENCHMARK = './bench/'
RESULT = './results/'

# Adjust tested settings here. Make sure if you add something, that you add a corresponding rule to provide the file or the approach
APPROACHES = [
    'itr',
]
DATA_TYPE = {
    'itr': '.nt'
}
DATA_SETS = [

]
DATA_TYPE_OF_DATA_SETS = {
    '': '.nt'
}

FILES = [f'indicators/{file}.{DATA_TYPE[approach]}.{approach}'
         for approach in APPROACHES
         for file in DATA_SETS
         ]

HYPERGRAPHS = [
    'contact-high-school',
    'contact-primary-school',
    'house-committees',
    'senate-bills',
    'senate-committees',
    'trivago-clicks',
    'walmart-trips'
]
# Necessary to create directories because output files of bwt construction are not named in snakemake file.
for path in [BENCHMARK, INPUT, TEMP, OUTPUT, INDICATORS, RESULT] + [OUTPUT + approach for approach in APPROACHES]:
    os.makedirs(path, exist_ok=True)

rule target:
    input:
        bench = 'results/benchmark.csv',
        stats = 'results/file_stats.csv'

rule get_results:
    input:
        set = FILES,
        hyprgraph_set=[f'split/{filename}.hypergraph.txt' for filename in HYPERGRAPHS]
    output:
        bench = 'results/benchmark.csv'
    run:
        """
        python3 scripts/collect_benchmarks.py bench {output.bench}
        """
        from scripts.collect_benchmarks import combine
        combine(DATA_SETS, APPROACHES, DATA_TYPE, output.bench)

rule stats:
    input:
        set = [f'split/{filename}.{DATA_TYPE_OF_DATA_SETS[filename]}' for filename in DATA_SETS],
    output:
        stats = 'results/file_stats.csv'
    shell:
        """
        python3 scripts/get_file_stats.py {output.stats} {input.set}
        """


rule clean:  # TODO: Clear installations and build repositories. NOTE: Does not clean results. You want them, I think...
    shell:
        f"""
        rm -rf {INPUT}
        rm -rf {TEMP}
        rm -rf {OUTPUT}
        rm -rf {INDICATORS}
        rm -rf {BENCHMARK}
        """

rule create_queries:  # TODO Number of triples unknown.
    input:
        script = 'k2Triples/build/k2triple',
        file = '{filename}'
    output:
        out_file = '{filename}_{type}.queries'
    shell:
        """
        {input.script} --make-query {filename} {filename}_{type}.queries 500 10000 {type}
        """

rule itr_query:
    input:
        script='itr/build/cgraph-cli',
        source='',
        querys=''
    output:
        indicator = 'indicator/{filename}.indicator.query'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}_queries.k2Triples.csv'
    shell:
        """
        {input.script} -hypergraph 
        """

rule k2triple_query:
    input:
        script = '',
        source = '',
        querys = ''
    output:
        indicator = ''
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}_queries.k2Triples.csv'
    shell:
        """
        {input.script} --use_tree {input.source} {input.query_count} {input.querys}
        """

rule itr:
    input:
        script = 'itr/build/cgraph-cli',
        source = 'input/{filename}'
    output:
        indicator = 'indicators/{filename}.ibb'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.itr.csv'
    shell:
        """if itr/build/cgraph-cli --max-rank 128 --rrr --sampling 0 --factor 64 --overwrite {input.source} data/itr/{input.source}; then
        echo 1 > {output.indicator}
        else
        echo 0 > {output.indicator}
        fi"""

rule itr_hypergraph:
    input:
        script='itr/build/cgraph-cli',
        source='input/{filename}'
    output:
        indicator = 'indicators/{filename}.ibb'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.itr.csv'
    shell:
        """if itr/build/cgraph-cli -f cornell_hypergraph --max-rank 128 --rrr --sampling 0 --factor 64 --overwrite {input.source} data/itr/{input.source}; then
        echo 1 > {output.indicator}
        else
        echo 0 > {output.indicator}
        fi"""

rule k2triples:
    input:
        script='k2Triples/build/k2triple',
        source='input/{filename}'
    output:
        indicator = 'indicators/{filename}.k2triples'
    params:
        threads = NUMBER_OF_PROCESSORS
    benchmark: 'bench/{filename}.k2triples.csv'
    shell:
        """
        mkdir data/itr/{input.source}
        if {input.script} -b {input.source} data/k2Triples/{input.source}/ 4 2 5 2; then
        echo 1 > {output.indicator}
        else
        echo 0 > {output.indicator}
        fi"""

rule d:
    input:
        script='rdf_indexes/build/build',
        source='input/{filename}'  # TODO: Needs the input as .gz-file
    output:
        indicator = 'indicators/{filename}.ibb'
    params:
        threads = NUMBER_OF_PROCESSORS,
        type = 'pef_3t' # Also possible: compact_3t ef_3t vb_3t pef_3t pef_r_3t pef_2to pef_2tp
    benchmark: 'bench/{filename}.itr.csv'
    shell: # TODO: Change input.source to just the base name for build.
        """
        rdf_indexes/process.sh {input.source}
        if rdf_indexes/build/build {params.type} {input.source} data/itr/{input.source}; then
        echo 1 > {output.indicator}
        else
        echo 0 > {output.indicator}
        fi"""

rule build_itr:
    output:
        script = 'itr/build/cgraph-cli'
    shell:
        """
        rm -rf ./itr
        sudo apt-get install libserd-0-0
        sudo apt-get install libmicrohttpd-dev
        rm -rf ./libdivsufsort
        git clone https://github.com/y-256/libdivsufsort.git
        cd libdivsufsort
        mkdir build
        cd build
        cmake -DCMAKE_BUILD_TYPE="Release" -DCMAKE_INSTALL_PREFIX="/usr/local" ..
        make
        sudo make install
        cd ../..
        git clone https://github.com/adlerenno/IncidenceTypeRePair
        mv IncidenceTypeRePair itr
        cd itr
        mkdir -p build
        cd build
        cmake -DCMAKE_BUILD_TYPE=Release -DOPTIMIZE_FOR_NATIVE=on -DWITH_RRR=on ..
        make
        """

rule build_k2triples:
    output:
        script = 'k2Triples/build/k2triple'
    shell:
        """
        unzip k2Triples.zip
        cd k2Triples
        mkdir -p build
        cd build
        cmake ..
        make
        chmod +x k2triple
        """

rule build_d:
    output:
        script = ''
    shell:
        """
        rm -rf rdf-indexes
        git clone --recursive https://github.com/jermp/rdf_indexes.git
        cd rdf_indexes 
        mkdir build
        cd build
        cmake ..
        make -j
        pip3 install mmh3 numpy
        """

rule fetch_jamendo:
    output:
        '{INPUT}/jamendo.nt'
    shell:
        """
        cd source
        wget http://moustaki.org/resources/jamendo-rdf.tar.gz
        tar -xf jamendo-rdf.tar.gz
        """

# TODO: Cite this, if used datasets.
# Generative hypergraph clustering: from blockmodels to modularity.
# Philip S. Chodrow, Nate Veldt, and Austin R. Benson.
# Science Advances, 2021.
# Code available at github.com/PhilChodrow/HypergraphModularity.
# Minimizing Localized Ratio Cut Objectives in Hypergraphs.
# Nate Veldt, Austin R. Benson, and Jon Kleinberg.
# Proceedings of the ACM SIGKDD International Conference on Knowledge Discovery and Data Mining (KDD), 2020.
# Code available at github.com/nveldt/HypergraphFlowClustering.
# Clustering in graphs and hypergraphs with categorical edge labels.
# Ilya Amburg, Nate Veldt, and Austin R. Benson.
# Proceedings of the Web Conference (WWW), 2020.
# Code available at github.com/nveldt/CategoricalEdgeClustering.

rule fetch_wallmart:
    output:
        'source/trivago-clicks/hyperedges-trivago-clicks.txt',
        'source/trivago-clicks/label-names-trivago-clicks.txt',
        'source/trivago-clicks/node-labels-trivago-clicks.txt',

        'source/trivago-clicks/hyperedges-walmart-trips.txt',
        'source/trivago-clicks/label-names-walmart-trips.txt',
        'source/trivago-clicks/node-labels-walmart-trips.txt',

        'source/trivago-clicks/hyperedges-contact-primary-school.txt',
        'source/trivago-clicks/label-names-contact-primary-school.txt',
        'source/trivago-clicks/node-labels-contact-primary-school.txt',

        'source/trivago-clicks/hyperedges-contact-high-school.txt',
        'source/trivago-clicks/label-names-contact-high-school.txt',
        'source/trivago-clicks/node-labels-contact-high-school.txt',

        'source/trivago-clicks/hyperedges-contact-house-committees.txt',
        'source/trivago-clicks/label-names-contact-house-committees.txt',
        'source/trivago-clicks/node-labels-contact-house-committees.txt',

        'source/trivago-clicks/hyperedges-contact-senate-bills.txt',
        'source/trivago-clicks/label-names-contact-senate-bills.txt',
        'source/trivago-clicks/node-labels-contact-senate-bills.txt',

        'source/trivago-clicks/hyperedges-contact-senate-committees.txt',
        'source/trivago-clicks/label-names-contact-senate-committees.txt',
        'source/trivago-clicks/node-labels-contact-senate-committees.txt'

    shell:
        """
        unzip source.zip
        """


