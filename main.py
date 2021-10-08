#!/usr/bin/env python

"""
This script populates tiledb database with sample data.
Each study has 2 arrays (for now):
{study_id}_tpm
{study_id}_sig
"""
import random
import numpy as np
import tiledb
import os
import shutil


def generate_study_index(es, index_id):
    pass


def generate_random_study(gene_ids, n_contrasts=20, max_fc=100):
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

    contrasts = []

    # generate the signatures for the study
    for contrast in contrast_ids:
        pvalues = np.random.default_rng().uniform(0, 1, len(gene_ids))
        fdr = np.random.default_rng().uniform(0, 1, len(gene_ids))
        logfc = np.random.default_rng().uniform(-100, 100, len(gene_ids))

        contrast = {
            "name": contrast,
            "formula": "~ A + B + C",
            "pvalues": pvalues,
            "fdr": fdr,
            "logfc": logfc
        }

        contrasts.append(contrast)

    # generate counts for the study
    tpm = np.random.default_rng().uniform(0, 10000, (len(gene_ids), 100))

    return study_id, contrasts, tpm


def insert_study_signatures(study_id, contrasts, gene_ids, base_uri=""):
    # define dimensions and domain
    genes_dim = tiledb.Dim(name="genes", domain=(1, len(gene_ids)), tile=1000, dtype=np.uint32)
    contrasts_dim = tiledb.Dim(name="contrasts", domain=(1, len(contrasts)), tile=3, dtype=np.uint32)
    domain = tiledb.Domain(genes_dim, contrasts_dim)

    # define attributes
    # noinspection PyTypeChecker
    pval_attr = tiledb.Attr(name="pvalue", dtype=np.float64)
    # noinspection PyTypeChecker
    fdr_attr = tiledb.Attr(name="fdr", dtype=np.float64)
    # noinspection PyTypeChecker
    logfc_attr = tiledb.Attr(name="logFC", dtype=np.float64)

    schema = tiledb.ArraySchema(domain=domain,
                                sparse=False,
                                attrs=[pval_attr, fdr_attr, logfc_attr],
                                cell_order='col-major',
                                tile_order='col-major')

    array_uri = os.path.join(base_uri, study_id + "_sig")
    tiledb.DenseArray.create(array_uri, schema)

    with tiledb.open(array_uri, 'w') as A:
        for i, contrast in enumerate(contrasts):
            print("burning witches")
            A[:, i + 1] = {"pvalue": contrast["pvalues"],
                           "fdr": contrast["fdr"],
                            "logFC": contrast["logfc"]}


def insert_study_values(study_id, docs):
    pass


def get_gene_ids():
    gene_ids = []

    # get a list of genes from the example file
    with(open("data/LNCap_DMSO_SP2509_24h_merged_reads.csv", 'r')) as f:
        header = f.readline().rstrip().split(",")

        for line in f:
            line = line.rstrip().split(",")
            gene_id = line[0]
            gene_ids.append(gene_id)
    return gene_ids


def main():
    """
    Generate a random study and insert it as a separate index into the database.
    Returns
    -------

    """
    # generate random dataset
    database_directory = "/home/suslik/Documents/programming/envision/backend/middle_layer/latest/database"
    shutil.rmtree(database_directory)
    os.mkdir(database_directory)
    base_uri = tiledb.group_create(os.path.join(database_directory, "statistics"))
    gene_ids = get_gene_ids()

    for i in range(1, 10):
        study_id, contrasts, tpm = generate_random_study(gene_ids, n_contrasts=5)
        insert_study_signatures(study_id, contrasts, gene_ids, base_uri=base_uri)


if __name__ == "__main__":
    main()
