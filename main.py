from elasticsearch import Elasticsearch


# This script populates elasticstore with the sample data.
# /study accession/contrast accession/formula/gene/pvalue/fdr/fold change/other usual deseq2 columns per gene/
# The procedure is the following:
# 1. Generate study id as GSF\d\d\d\d
# 2. Generate contrast accession as GSC\d\d\d\d_n, where \d\d\d\d is the same as in GFS\d\d\d\d of the parent study
# 3. Generate formula (as a random formula from the list of the 10 different variables)
# 4. Generate p-value and FC


def generate_random_study_object():
    return


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
                "gene": {"type": "keyword"},
                "sample": {"type": "keyword"},
                "value": {"type": "integer"}
            }
        }
    }

    # fully delete the old index and create a new one
    es.indices.delete(index=index_id, ignore=[400, 404])
    es.indices.create(index=index_id, body=body, ignore=400)


if __name__ == "__main__":
    es = Elasticsearch()

    index_id = "studies"
    print("Populating pstore:")
    print("Generating index...")
    generate_study_index(es)
