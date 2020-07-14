import boto3
import socket
import time
import configparser


def create_bastion_host():
    """
    Create bastion host to ssh tunnel to RedShift master node

    Parameters:
    None

    Returns:
    public_dns_name (string): Public DNS address of bastion host
    """

    # Obtain bastion host configuration parameters
    config = configparser.ConfigParser()

    # Tell ConfigParser class to pass through options unchanged
    # Source: https://stackoverflow.com/questions/19359556/configparser-reads-capital-keys-and-make-them-lower-case
    config.optionxform = str

    config.read_file(open('redshift.cfg'))

    ec2_key = config.get("BASTION", "KEY_NAME")
    ec2_subnet_id = config.get("BASTION", "SUBNET_ID")
    ec2_group_id = config.get("BASTION", "GROUP_ID")

    retries = 10
    retry_delay=10
    retry_count = 0

    ec2 = boto3.client('ec2', region_name="us-west-2")

    # create a new EC2 instance
    response = ec2.run_instances(
         ImageId= 'ami-0b1e2eeb33ce3d66f',
         MinCount= 1,
         MaxCount= 1,
         InstanceType= 't2.micro',
         KeyName= ec2_key,
         NetworkInterfaces = [
        {
            'SubnetId': ec2_subnet_id,
            'DeviceIndex': 0,
            'AssociatePublicIpAddress': True,
            'Groups': [ec2_group_id]
        }
        ]
     
     )

    # Adapted from https://stackoverflow.com/questions/46379043/boto3-wait-until-running-doesnt-work-as-desired
    ec2 = boto3.resource('ec2',region_name="us-west-2")
    instance = ec2.Instance(id=response["Instances"][0]["InstanceId"])

    instance.wait_until_running()

    public_dns_name = instance.public_dns_name

    print(public_dns_name)

    while retry_count <= retries:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex((instance.public_ip_address,22))
        if result == 0:
            print("Instance is UP & accessible on port 22, the IP address is:  " + instance.public_ip_address)
            break
        else:
            print ("instance is still down retrying . . . ")
            time.sleep(retry_delay)

    # Write public DNS address to redshift.cfg
    # Adapted from https://stackoverflow.com/questions/27964134/change-value-in-ini-file-using-configparser-python
    config.set("BASTION", "PUBLIC_DNS", public_dns_name)
    
    with open("redshift.cfg", "w") as configfile:
        config.write(configfile)

    return public_dns_name


def main():
    """
    Create bastion host to ssh tunnel to RedShift master node

    Parameters:
    None

    Returns:
    None
    """

    create_bastion_host()

if __name__ == "__main__":
    main()

