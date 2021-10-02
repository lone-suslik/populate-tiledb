#!/usr/bin/env python

# This script populates elasticstore with the sample data.
# The schema is defined in the generate_study_index function
# The random study generation is defined in the generate_random_study function
import random

from elasticsearch import Elasticsearch


def generate_study_index(es):
    """
    Create a new ES index and populate it with random data

    Parameters
    ----------
    es: Elasticsearch
        Elasticsearch instance from Elasticsearch package

    Returns
    -------
    """

    # define mapping schema
    body = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties": {
                "study": {"type": "keyword"},
                "contrast": {
                    "properties": {
                        "gene": {
                            "properties": {
                                "id": {"type": "keyword"},
                                "pvalue": {"type": "float"},
                                "fdr": {"type": "float"},
                                "logFC": {"type": "float"}
                            }
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "formula": {
                            "type": "text"
                        }
                    }
                }
            }
        }
    }

    # fully delete the old index and create a new one
    es.indices.delete(index=index_id, ignore=[400, 404])
    res = es.indices.create(index=index_id, body=body, ignore=400)

    print(res)


def generate_random_study():
    """
    The procedure is the following:
    1. Generate study id as GSF\d\d\d\d
    2. Generate contrast accession as GSC\d\d\d\d_n, where \d\d\d\d is the same as in GFS\d\d\d\d of the parent study
    3. Generate formula (as a random formula from the list of the 10 different variables)
    4. Generate p-value and FC

    Returns
    -------

    """

    study_id = "GSF" + str(random.randrange(1000, 9999))

    # get a list of genes from the example file
    doc = {
        "study": study_id,
        "contrast": {
            "gene": [
                {
                    "id": "a",
                    "pvalue": 0.01,
                    "fdr": 0.01,
                    "logFC": 5
                },
                {
                    "id": "b",
                    "pvalue": 0.04,
                    "fdr": 0.7,
                    "logFC": 3.2
                },
            ],
            "name": "first contrast",
            "formula": "~ A + B + C"
        }
    }

    return doc


if __name__ == "__main__":
    es = Elasticsearch()

    index_id = "studies"
    print("Populating pstore:")
    print("Generating index...")

    # create index
    generate_study_index(es)

    # generate random study
    doc = generate_random_study()

    # add study to index
    res = es.index(index=index_id, body=doc)
    print(res)
