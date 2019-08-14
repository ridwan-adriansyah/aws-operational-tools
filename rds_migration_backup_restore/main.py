import boto3
from sys import argv
import time
client = boto3.client('rds')

"""
This script is used to create and shared encrypted RDS snapshot from db instance to other AWS account.
To use this script simply run
`python shared_encrypted_rds_snapshot.py db_snapshot_identifier kms_key_id account_id`
where: 
- db_instance_id is the RDS db instance identifer that need to be snapshot.
- kms_key_id, arn of kms_key_id that used to copy and reencrypt the snapshot.
- account id is the target account id to share the snapshot.

example:
python shared_encrypted_rds_snapshot.py myprod_rds_id arn:aws:kms:ap-southeast-1:11223344:key/aabbcc-221-33-11-44 1122334455
"""


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print '%r  %2.2f s' % \
            (method.__name__, (te - ts))
        return result
    return timed


def share_snapshot(snapshot_arn, dest_account_id):
    """
    Modify snapshot attribute, to share snapshot `snapshot_arn` 
    to `dest_account_id`
    """
    print("sharing snapshot " + snapshot_arn +
          " to account " + dest_account_id)
    try:
        response = client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snapshot_arn,
            AttributeName='restore',
            ValuesToAdd=[
                dest_account_id,
            ],
        )
    except Exception as e:
        print e

    print("Snapshot " + snapshot_arn + " shared to " + dest_account_id)


@timeit
def copy_snapshot(source_snapshot_arn, kms_key_id, tg_snapshot_id):
    """
    Copy snapshot from `source_snapshot_arn` encrypt it with `kms_key_id` and 
    with snapshot identifier `db_instance_id` + "-to-be-shared"
    and wait for snapshot to be available.
    """

    print("copying snapshot " + source_snapshot_arn + " and ecrypt with KMS key " +
          kms_key_id + " to " + tg_snapshot_id + "-to-be-shared")
    target_snapshot_id = ""
    try:

        response = client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=source_snapshot_arn,
            TargetDBSnapshotIdentifier=tg_snapshot_id + '-to-be-shared',
            KmsKeyId=kms_key_id,
            CopyTags=True
        )
        target_snapshot_id = response["DBSnapshot"]["DBSnapshotIdentifier"]
        if target_snapshot_id != "":
            wait_snapshot_available(response["DBSnapshot"][
                                    "DBSnapshotIdentifier"])

    except Exception as e:
        print e

    return target_snapshot_id


def wait_snapshot_available(snapshot_id):
    print("Waiting for snapshot " + snapshot_id + " to be available")
    waiter = client.get_waiter('db_snapshot_available')
    waiter.wait(
        DBSnapshotIdentifier=snapshot_id,
        WaiterConfig={
            'Delay': 5,
            'MaxAttempts':  1000
        }
    )


# check if db instance encrypted or not, if not, create the snapshot using shared kms key.
# if yes, check if the db is encrpted using default rds/kms key, if yes create the db snapshot using default rds/kms key, and then copy the snapshot using shared kms key.
# if not,  you can just create the snapshot and share the snapshot and the
# kms key used to encrypt the database to target account
@timeit
def create_snapshot(db_instance_id):
    """
    Create snapshot from db_instance with snapshot identifier 
    `db_instance_id` + "-to-be-copied", and wait for it to be available.
    """
    print("creating snapshot from " + db_instance_id +
          " to " + db_instance_id + "-to-be-copied")
    snapshot_arn = ""
    try:
        response = client.create_db_snapshot(
            DBSnapshotIdentifier=db_instance_id + '-to-be-copied',
            DBInstanceIdentifier=db_instance_id,
        )
        snapshot_arn = response["DBSnapshot"]["DBSnapshotArn"]
        if snapshot_arn != "":
            wait_snapshot_available(response["DBSnapshot"][
                                    "DBSnapshotIdentifier"])
    except Exception as e:
        print(e)
    return snapshot_arn


def run():
    if len(argv) < 4:
        print("Usage: main.py <db-instance-id> <kms-key-id> <destination-aws-account-id>")
        exit(-1)
    db_instance_id = argv[1]
    kms_key_id = argv[2]
    dest_account_id = argv[3]

    snapshot_arn = create_snapshot(db_instance_id)

    if snapshot_arn == "":
        return

    shared_snapshot_id = copy_snapshot(
        snapshot_arn, kms_key_id, db_instance_id)

    if shared_snapshot_id == "":
        return
    time.sleep(10)
    share_snapshot(shared_snapshot_id, dest_account_id)


if __name__ == "__main__":
    run()
