#!/usr/bin/env python

# This script populates elasticstore with the sample data.
# The schema is defined in the generate_study_index function
# The random study generation is defined in the generate_random_study function
import random

from elasticsearch import Elasticsearch
from elasticsearch import helpers


def generate_study_index(es, index_id):
    """
    Create a new ES index using the following mappings:

        "mappings": {
            "properties": {
                "study": {"type": "keyword"},
                "contrast": {
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "formula": {
                            "type": "text"
                        }
                    }
                },
                "gene": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "pvalue": {"type": "float"},
                        "fdr": {"type": "float"},
                        "logFC": {"type": "float"}
                    }
                },
            }
        }

    Parameters
    ----------
    es: Elasticsearch
        Elasticsearch instance from Elasticsearch package
    index_id: str

    Returns
    -------
    """

    # define mapping schema
    # max_rescore_window index settings is 200000
    body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "max_rescore_window": 200000,
            "max_result_window": 200000,  # that should cover most of the transcript level data
        },
        "mappings": {
            "properties": {
                "contrast": {
                    "type": "nested",
                    "properties": {
                        "name": {
                            "type": "keyword"
                        },
                        "formula": {
                            "type": "text"
                        }
                    }
                },
                "gene": {
                    "properties": {
                        "id": {"type": "keyword"},
                        "pvalue": {"type": "float"},
                        "fdr": {"type": "float"},
                        "logFC": {"type": "float"}
                    }
                },
            }
        }
    }

    # fully delete the old index and create a new one
    es.indices.delete(index=index_id, ignore=[400, 404])
    res = es.indices.create(index=index_id, body=body, ignore=400)

    print(res)


def generate_random_study(gene_ids, n_contrasts=5, max_fc=100):
    """
    Generate a list of random json documents, one document per gene id.
    Documents correspond to mappings from generate_study_index function.
    Study id is generated as gsf\d\d\d\d
    Contrast is generated  gsc\d\d\d\d\d, where first 4 \d\d\d\d is the same as in gsf\d\d\d\d of the parent study
    Parameters
    ----------
    max_fc: int, default=100
    n_contrasts: int, default=5
gene_ids: list of str
    Returns
    -------
    study_id:
        the random name of the study;
    docs:
        list of jsons, one json per gene id
    """

    study_tag = str(random.randrange(1000, 9999))
    study_id = "gsf" + study_tag
    contrast_ids = ["gsc" + study_tag + str(x) for x in range(0, n_contrasts)]

    docs = []

    # generate the index for the study
    for contrast in contrast_ids:

        contrast = {
            "name": contrast,
            "formula": "~ A + B + C",
        }

        for gene_id in gene_ids:
            gene = {
                "id": gene_id,
                "pvalue": random.random(),
                "fdr": random.random(),
                "logFC": random.random() * max_fc
            }

            docs.append({
                # "study": study_id, # - comment this out if we don't want study id as a separate field for each index
                "contrast": contrast,
                "gene": gene
            })

    return study_id, docs


def insert_random_study(es, index_id, docs):
    """
    Insert a list of documents into an index indicated by index_id.


    Parameters
    ----------
    es: Elasticsearch
        Elasticsearch object
    index_id: str
              Name of the index to which to insert the docs,
    docs:

    Returns
    -------
    json:
        Elasticsearch response json object
    """

    # add study to index
    # this is the way to invoke the bulk api as described at:
    # https://elasticsearch-py.readthedocs.io/en/master/helpers.html#example
    def iterate_docs():
        for d in docs:
            yield {
                "_index": index_id,
                "_type": "_doc",
                "_source": d
            }

    return helpers.bulk(es, iterate_docs())


def main():
    """
    Generate a random study and insert it as a separate index into the database.
    Returns
    -------

    """
    es = Elasticsearch()

    # index_id = "studies"
    print("Populating pstore:")
    print("Generating index...")

    gene_ids = []

    # get a list of genes from the example file
    with(open("data/LNCap_DMSO_SP2509_24h_merged_reads.csv", 'r')) as f:
        header = f.readline().rstrip().split(",")

        for line in f:
            line = line.rstrip().split(",")
            gene_id = line[0]
            gene_ids.append(gene_id)

    # generate n random studies
    for i in range(0, 10):
        print(f"Inserting study {i}")
        study_id, docs = generate_random_study(gene_ids)

        # create index
        generate_study_index(es, study_id)  # use study_id as index id
        # insert study into index
        insert_random_study(es, study_id, docs)  # use study_id as index id


if __name__ == "__main__":
    main()
