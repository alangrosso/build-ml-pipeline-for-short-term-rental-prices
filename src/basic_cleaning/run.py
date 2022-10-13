#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning") # project="nyc_airbnb"
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################

    # Download data
    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()

    df = pd.read_csv(artifact_path)

    # Drop outliers
    logger.info("Drop outliers: min_price and max_price")
    min_price = 10
    max_price = 350
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    
    # Convert last_review to datetime
    logger.info("Convert last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Filename to export
    filename = "clean_sample.csv"
    df.to_csv(filename, index=False) # "clean_sample.csv"
    
    # Upload artifact to W&B
    logger.info("Upload filename to W&B")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
        )
    artifact.add_file(filename) # "clean_sample.csv"
    
    # Logging artifact
    logger.info("Logging artifact")
    run.log_artifact(artifact)

    # Remove artifact
    os.remove(filename)

    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str, 
        help="Name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str, 
        help="Name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str, 
        help="Type for the produced artifacts",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str, 
        help="Description of the the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="An integer number that truncate to a minimun value",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="An integer number that truncate to a maximum value",
        required=True
    )


    args = parser.parse_args()

    go(args)
