import pandas as pd
import boto3
import json
import configparser


def create_redshift_cluster():
    """
    Create AWS RedShift cluster

    Parameters:
    None

    Returns:
    None
    """

    config = configparser.ConfigParser()

    # Tell ConfigParser class to pass through options unchanged
    # Source: https://stackoverflow.com/questions/19359556/configparser-reads-capital-keys-and-make-them-lower-case
    config.optionxform = str

    config.read_file(open('/tmp/redshift.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")
    DWH_REGION             = config.get("DWH", "DWH_REGION")
    DWH_CLUSTER_SUBNET_NAME= config.get("DWH", "DWH_CLUSTER_SUBNET_NAME")
    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    DWH_ROLE_ARN           = config.get("DWH", "DWH_ROLE_ARN")
    DWH_CLUSTER_SUBNET_GROUP_NAME = config.get("DWH", "DWH_CLUSTER_SUBNET_GROUP_NAME")

    # Create client for RedShift
    redshift = boto3.client('redshift',
                           region_name=DWH_REGION,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

    # Create client to create IAM role to access S3 resources
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=DWH_REGION
                      )


    # Create the IAM role 
    try:
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
        
    print("Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    # Write newly created ARN to IAM_ROLE ARN variable in redshift.cfg
    # Adapted from https://stackoverflow.com/questions/27964134/change-value-in-ini-file-using-configparser-python
    config.set("DWH", "IAM_ROLE_ARN", roleArn)

    with open("/tmp/redshift.cfg", "w") as configfile:
        config.write(configfile)

    # Create RedShift cluster
    try:
        response = redshift.create_cluster(       
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            ClusterSubnetGroupName=DWH_CLUSTER_SUBNET_GROUP_NAME, # Used to place cluster in specified VPC. You must have access to said VPC (i.e. VPN, bastion host, etc.)

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)                       


    # Wait for cluster to start
    waiter = redshift.get_waiter('cluster_available')
    waiter.wait( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER )


    # Obtain private ip within VPC to connect via VPN
    myClusterLeaderNode = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    for clusterNode in myClusterLeaderNode["ClusterNodes"]:
        if clusterNode["NodeRole"] == "LEADER":
            DWH_ENDPOINT = clusterNode["PrivateIPAddress"]


    # Write host to redshift.cfg
    # Adapted from https://stackoverflow.com/questions/27964134/change-value-in-ini-file-using-configparser-python
    config.set("DWH", "DWH_HOST", DWH_ENDPOINT)

    with open("/tmp/redshift.cfg", "w") as configfile:
        config.write(configfile)

def main():
    """
    Create AWS RedShift cluster

    Parameters:
    None

    Returns:
    None
    """

    # Create AWS RedShift cluster
    create_redshift_cluster()

if __name__ == "__main__":
    main()
