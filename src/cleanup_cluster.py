import boto3
import configparser


def cleanup_cluster(run_local = True, **kwargs):

    if not run_local:
        # Exit cleanup_cluster 
        if not kwargs['dag_run'].conf['delete_redshift_cluster']:
            return 

    # Obtain RedShift credentials
    config = configparser.ConfigParser()
    config.read_file(open('/tmp/redshift.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_REGION             = config.get("DWH", "DWH_REGION")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


    config = configparser.ConfigParser()
    config.read_file(open('/tmp/bastion.cfg'))
    ec2_bastion_instance_id = config.get("BASTION", "INSTANCE_ID")

    # Delete RedShift cluster
    redshift = boto3.client('redshift',
                           region_name=DWH_REGION,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
 
    # Delete RedShift cluster. No need for final snapshot.
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
 
    # Delete IAM role
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=DWH_REGION
                      )
 
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

    # Delete ssh bastion host
    ids = [ec2_bastion_instance_id]
    ec2 = boto3.resource('ec2',
                         region_name=DWH_REGION,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                         )

    ec2.instances.filter(InstanceIds = ids).terminate()

def main():
    cleanup_cluster()

if __name__ == "__main__":
    main()
