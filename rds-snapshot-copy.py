import boto3
import time
import operator
import datetime
import logging
import os

RDS_DBINSTANCEIDENTIFIER = "prd-rds"
SNAPSHOT_RETENTION = 30

logger = logging.getLogger(name=__name__)
env_level = os.environ.get("LOG_LEVEL")
log_level = logging.INFO if not env_level else env_level
logger.setLevel(log_level)

def list_snapshots():
    snapshot_count = 0
    client = boto3.client('rds', 'us-west-2')
    response = client.describe_db_snapshots(
        SnapshotType='shared',
        IncludeShared=True,
        IncludePublic=False,
    )

    if len(response['DBSnapshots']) == 0:
        raise Exception("No snapshots found")

    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            continue

        if snapshot['DBInstanceIdentifier'] == RDS_DBINSTANCEIDENTIFIER:
            snapshots_per_project[snapshot['DBSnapshotIdentifier']] = [snapshot['DBSnapshotArn'],snapshot['SnapshotCreateTime'],snapshot['KmsKeyId']]
            snapshot_count +=1    
            
def copy_snapshots():
    snapshots_copied = 0
    client = boto3.client('rds', 'us-west-2')
    response = client.describe_db_snapshots(
        SnapshotType='shared',
        IncludeShared=True,
        IncludePublic=False,
    )

    if len(response['DBSnapshots']) == 0:
        raise Exception("No shared snapshots found.")

    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            continue

        if snapshot['DBInstanceIdentifier'] == RDS_DBINSTANCEIDENTIFIER:
            snapshots_per_project[snapshot['DBSnapshotIdentifier']] = [snapshot['DBSnapshotArn'],snapshot['SnapshotCreateTime'],snapshot['KmsKeyId']]
            copy_name = RDS_DBINSTANCEIDENTIFIER + "-shield-" + snapshots_per_project[snapshot['DBSnapshotIdentifier']][1].strftime("%Y%m%d%H%M%S")

            logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " : " + "Checking if " + copy_name + " is copied...")

            try:
                client.describe_db_snapshots(
                    DBSnapshotIdentifier=copy_name,
                    SnapshotType='manual',
                    IncludeShared=True,
                    IncludePublic=False,
                )
            except:
                logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " : " + "Starting copy for " + copy_name)

                response = client.copy_db_snapshot(
                    SourceDBSnapshotIdentifier=snapshots_per_project[snapshot['DBSnapshotIdentifier']][0],
                    TargetDBSnapshotIdentifier=copy_name,
                    KmsKeyId='{}'.format(snapshots_per_project[snapshot['DBSnapshotIdentifier']][2]),
                    CopyTags=False
                )

                waiter = client.get_waiter('db_snapshot_completed')
                waiter.config.delay = 30
                waiter.config.max_attempts = 60    
                waiter.wait(
                    DBInstanceIdentifier=snapshot['DBInstanceIdentifier'],
                    DBSnapshotIdentifier='{}'.format(copy_name),
                    SnapshotType='manual',
                    MaxRecords=100,
                    IncludeShared=True,
                    IncludePublic=False,
                )
                
                response = client.describe_db_snapshots(
                    DBSnapshotIdentifier=copy_name,
                    SnapshotType='manual',
                    IncludeShared=True,
                    IncludePublic=False,
                )
                logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " : " + copy_name + " snapshot status is " + str(response['DBSnapshots'][0]['Status']) + ".")

                if response['DBSnapshots'][0]['Status'] != "available":
                     raise Exception("Copy operation for " + copy_name + " failed!")
                logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " : " + "Copied " + copy_name)
                snapshots_copied +=1

                continue

            logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " : " + copy_name + " has already been copied.")
    logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " : " + "Snapshots copied: " + str(snapshots_copied))

# def delete_snapshots accepts parameters of 'manual' or 'shared'
def delete_snapshots(rds_snapshottype):
    snapshot_count = 0
    client = boto3.client('rds', 'us-west-2')
    response = client.describe_db_snapshots(
        SnapshotType='{}'.format(rds_snapshottype),
        IncludeShared=True,
        IncludePublic=False,
    )

    if len(response['DBSnapshots']) == 0:
        raise Exception("No shared snapshots found.")

    snapshots_per_project = {}
    for snapshot in response['DBSnapshots']:
        if snapshot['Status'] != 'available':
            continue

        # Filter snapshots based on RDS_DBINSTANCEIDENTIFIER
        if snapshot['DBInstanceIdentifier'] == RDS_DBINSTANCEIDENTIFIER:
            snapshot_count +=1

            # manual snapshots can use DBSnapshotIdentifier
            if rds_snapshottype == 'manual':
                if snapshot['DBInstanceIdentifier'] not in snapshots_per_project.keys():
                    snapshots_per_project[snapshot['DBInstanceIdentifier']] = {}    
                snapshots_per_project[snapshot['DBInstanceIdentifier']][snapshot['DBSnapshotIdentifier']] = snapshot[
                    'SnapshotCreateTime']
            # shared snapshots must use DBSnapshotArn
            elif rds_snapshottype == 'shared':
                if snapshot['DBInstanceIdentifier'] not in snapshots_per_project.keys():
                    snapshots_per_project[snapshot['DBInstanceIdentifier']] = {}    
                snapshots_per_project[snapshot['DBInstanceIdentifier']][snapshot['DBSnapshotArn']] = snapshot[
                    'SnapshotCreateTime']

    for project in snapshots_per_project:
        if snapshot_count > SNAPSHOT_RETENTION:
            sorted_list = sorted(snapshots_per_project[project].items(), key=operator.itemgetter(1), reverse=True)
            to_remove = [i[0] for i in sorted_list[SNAPSHOT_RETENTION:]]

            for snapshot in to_remove:
                logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": " + "Removing " + snapshot)
                client.delete_db_snapshot(
                    DBSnapshotIdentifier=snapshot
                )
        else:
            logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ": " + "No snapshots to delete.")

def lambda_handler(event, context):
    delete_snapshots('manual')
    delete_snapshots('shared')
    list_snapshots()
    copy_snapshots()

if __name__ == '__main__':
