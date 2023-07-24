#!/usr/bin/env python

"""
This script populates tiledb database with sample data.
Each study has 2 arrays (for now):
{study_id}_tpm
{study_id}_sig
"""
import random
import numpy as np
import statsmodels.stats.multitest as sm
import tiledb
import os
import shutil
from typing import List, Optional
from sklearn.utils.extmath import cartesian


def generate_random_study(gene_ids: List,
                          n_contrasts: int = 20,
                          max_fc: int = 100):
    """
    Generate a list of random json documents, one document per gene id.
    Documents correspond to mappings from generate_study_index function.

    Parameters
    ----------
    max_fc: int, default=100
            Absolute maximum value of FC
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
        fdr = sm.fdrcorrection(pvalues)[1]
        logfc = np.random.default_rng().uniform(-max_fc, max_fc, len(gene_ids))

        contrast = {
            "id": contrast,
            "formula": "~ A + B + C",
            "pvalues": pvalues,
            "fdr": fdr,
            "logfc": logfc
        }

        contrasts.append(contrast)

    # generate counts for the study
    tpm = np.random.default_rng().uniform(0, 10000, (len(gene_ids), 100))
    sample_ids = ["sample_" + str(x) for x in range(0, 100)]

    return study_id, contrasts, tpm, sample_ids


def insert_study_stats(contrasts, gene_ids, base_uri=""):
    # TODO check cell order
    # define dimensions and domain
    genes_dim = tiledb.Dim(name="gene", tile=1000, dtype=np.bytes_)
    contrasts_dim = tiledb.Dim(name="contrast", tile=3, dtype=np.bytes_)
    domain = tiledb.Domain(genes_dim, contrasts_dim)

    # define attributes
    # noinspection PyTypeChecker
    pval_attr = tiledb.Attr(name="pvalue", dtype=np.float64)
    # noinspection PyTypeChecker
    fdr_attr = tiledb.Attr(name="fdr", dtype=np.float64)
    # noinspection PyTypeChecker
    logfc_attr = tiledb.Attr(name="logFC", dtype=np.float64)

    schema = tiledb.ArraySchema(domain=domain,
                                sparse=True,
                                attrs=[pval_attr, fdr_attr, logfc_attr],
                                cell_order='col-major',
                                tile_order='col-major')

    stats_uri = os.path.join(base_uri, "stats")

    # create statistics array
    tiledb.SparseArray.create(stats_uri, schema)
    with tiledb.open(stats_uri, 'w') as A:
        for i, contrast in enumerate(contrasts):
            print("Burning witches")
            contrast_coords = [contrast['id']] * len(gene_ids)  # required for tiledb sparse writes
            A[gene_ids, contrast_coords] = {"pvalue": contrast["pvalues"],
                                            "fdr": contrast["fdr"],
                                            "logFC": contrast["logfc"]}


def insert_study_contrasts(contrasts: List,
                           base_uri: Optional[str] = ""):
    """
    Insert contrast information into a sparce array with only one dimension - contrastid
    Parameters
    ----------
    base_uri
    study_id
    contrasts

    Returns
    -------

    """
    contrasts_dim = tiledb.Dim(name="contrast", dtype=np.bytes_)
    domain = tiledb.Domain(contrasts_dim)
    formula_attr = tiledb.Attr(name="formula", dtype=np.bytes_)

    schema = tiledb.ArraySchema(domain=domain,
                                sparse=True,
                                attrs=[formula_attr])

    contrasts_uri = os.path.join(base_uri, "contrasts")
    tiledb.SparseArray.create(contrasts_uri, schema)

    with tiledb.open(contrasts_uri, 'w') as A:
        print("Administering but a scratches")
        for contrast in contrasts:
            A[contrast["id"]] = contrast["formula"]


def insert_study_values(sample_ids, gene_ids, tpm, base_uri=""):
    samples_dim = tiledb.Dim(name="sample", dtype=np.bytes_)
    genes_dim = tiledb.Dim(name="gene", tile=1000, dtype=np.bytes_)
    domain = tiledb.Domain(genes_dim, samples_dim)
    expr_attr = tiledb.Attr(name="expr", dtype=np.float64)

    schema = tiledb.ArraySchema(domain=domain,
                                sparse=True,
                                attrs=[expr_attr],
                                cell_order='row-major',
                                tile_order='row-major')

    tpm_uri = os.path.join(base_uri, "tpm")

    # create tpm array
    tiledb.SparseArray.create(tpm_uri, schema)

    # compute cartesian product of axis labels for data insertion
    insert_coordinates = cartesian((gene_ids, sample_ids))

    with tiledb.open(tpm_uri, 'w') as A:
        print("Hanging on to an imperialist dogma")
        A[insert_coordinates[:, 0], insert_coordinates[:, 1]] = tpm.flatten()


def insert_study_values_dense(sample_ids, gene_ids, tpm, base_uri=""):
    genes_dim = tiledb.Dim(name="gene", domain=(1, len(gene_ids)), tile=1000, dtype=np.int32)
    samples_dim = tiledb.Dim(name="sample", domain=(1, len(sample_ids)), dtype=np.int32)

    domain = tiledb.Domain(genes_dim, samples_dim)
    expr_attr = tiledb.Attr(name="expr", dtype=np.float64)

    schema = tiledb.ArraySchema(domain=domain,
                                sparse=False,
                                attrs=[expr_attr],
                                cell_order='row-major',
                                tile_order='row-major')

    tpm_uri = os.path.join(base_uri, "tpm_dense")

    # create tpm array
    tiledb.DenseArray.create(tpm_uri, schema)

    with tiledb.open(tpm_uri, 'w') as A:
        print("Performing routines and chorus scenes with footwork impeccable")
        A[:] = tpm


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
    base_uri = tiledb.group_create(os.path.join(database_directory))
    gene_ids = get_gene_ids()

    for i in range(1, 10):
        study_id, contrasts, tpm, sample_ids = generate_random_study(gene_ids, n_contrasts=5)

        group_uri = os.path.join(base_uri, study_id)
        tiledb.group_create(group_uri)

        insert_study_stats(contrasts, gene_ids, base_uri=group_uri)
        insert_study_contrasts(contrasts, base_uri=group_uri)
        insert_study_values(sample_ids, gene_ids, tpm, base_uri=group_uri)
        insert_study_values_dense(sample_ids, gene_ids, tpm, base_uri=group_uri)


if __name__ == "__main__":
    main()
