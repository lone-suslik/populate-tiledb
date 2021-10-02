#!/usr/bin/env python

# This script populates elasticstore with the sample data.
# The schema is defined in the generate_study_index function
# The random study generation is defined in the generate_random_study function
import random

from elasticsearch import Elasticsearch


def generate_study_index(es, index_id):
    """
    Create a new ES index and populate it with random data

    Parameters
    ----------
    es: Elasticsearch
        Elasticsearch instance from Elasticsearch package
    index_id: str

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


def generate_random_study(gene_ids, n_contrasts=5, max_fc=100):
    """
    The procedure is the following:
    1. Generate study id as GSF\d\d\d\d
    2. Generate contrast accession as GSC\d\d\d\d_n, where \d\d\d\d is the same as in GFS\d\d\d\d of the parent study
    3. Generate formula (as a random formula from the list of the 10 different variables)
    4. Generate p-value and FC
    Parameters
    ----------
    max_fc: int, default=100
    n_contrasts: int, default=5
    gene_ids: list of str
    Returns
    -------

    """

    study_tag = str(random.randrange(1000, 9999))
    study_id = "GSF" + study_tag
    contrast_ids = ["GSFC" + study_tag + str(x) for x in range(0, n_contrasts)]

    contrasts = []

    for contrast in contrast_ids:
        genes = []

        for gene in gene_ids:
            genes.append({
                "id": gene,
                "pvalue": random.random(),
                "fdr": random.random(),
                "logFC": random.random() * max_fc
            })

        contrast = {
            "name": contrast,
            "formula": "~ A + B + C",
            "gene": genes
        }

        contrasts.append(contrast)

    doc = {
        "study": study_id,
        "contrast": contrasts
    }

    return doc


def main():
    es = Elasticsearch()

    index_id = "studies"
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

    # create index
    generate_study_index(es, index_id)

    # generate random study
    doc = generate_random_study(gene_ids)

    # add study to index
    res = es.index(index=index_id, body=doc)
    print(res)


if __name__ == "__main__":
    main()
