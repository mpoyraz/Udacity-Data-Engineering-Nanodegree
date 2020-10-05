import argparse
import configparser
import json
import time
import boto3

def create_clients(access_key_id, secret_access_key, region_name):
    """ Creates boto3 resource for EC2 and clients for IAM and Redshift.
    
    Args:
    access_key_id (str): AWS access key id
    secret_access_key (str): AWS secret access key
    region_name (str): AWS region name
    
    Returns:
    ec2_resource: boto3 resource for EC2
    iam_client: boto3 client for IAM
    redshift_client: boto3 client for Redshift
    """
    ec2_resource = boto3.resource('ec2', region_name = region_name,
                         aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    iam_client = boto3.client('iam', region_name = region_name,
                       aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    redshift_client = boto3.client('redshift', region_name = region_name,
                            aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
    return ec2_resource, iam_client, redshift_client

def check_iam_role_exists(iam_client, iam_role_name):
    """ Check if given IAM role exists.
    
    Args:
    iam_client (boto3 IAM client)
    iam_role_name (str): IAM role name
    
    Returns: True or False
    """
    try:
        iam_client.get_role(RoleName=iam_role_name)
        return True
    except iam_client.exceptions.NoSuchEntityException:
        return False

def delete_iam_role(iam_client, iam_role_name):
    """ Deletes given IAM role if it exists.
    
    Args:
    iam_client (boto3 IAM client)
    iam_role_name (str): IAM role name
    """
    # Delete attached policies to the IAM role
    response = iam_client.list_attached_role_policies(RoleName=iam_role_name)
    attached_policies = response['AttachedPolicies']
    for attached_policy in attached_policies:
        iam_client.detach_role_policy(RoleName=iam_role_name,
                                      PolicyArn=attached_policy['PolicyArn'])
    # Delete the IAM role
    iam_client.delete_role(RoleName=iam_role_name)
    print('IAM role \'{}\' is deleted along with its attached policies'.format(iam_role_name))

def create_iam_role_with_policy(iam_client, iam_role_name, policy_arn):
    """ Creates a new IAM role and attaches the given policy.
    
    Args:
    iam_client (boto3 IAM client)
    iam_role_name (str): IAM role name
    policy_arn (str): IAM policy ARN
    """
    # if the given role already exists, delete it
    if check_iam_role_exists(iam_client, iam_role_name):
        print('IAM role \'{}\' already exists, will delete it'.format(iam_role_name))
        delete_iam_role(iam_client, iam_role_name)
    # Create a new IAM role
    new_iam_role = iam_client.create_role(
        Path='/',
        RoleName=iam_role_name,
        AssumeRolePolicyDocument=json.dumps(
            {"Version": "2012-10-17",
             "Statement": [{ "Effect": "Allow",
                             "Principal": { "Service": ["redshift.amazonaws.com"] },
                             "Action": ["sts:AssumeRole"] }]
            }
        ),
        Description='Allows Redshift clusters to call AWS services on your behalf',
    )
    print('New IAM role \'{}\' is successfully created'.format(iam_role_name))
    # Attach
    iam_client.attach_role_policy(RoleName=iam_role_name, PolicyArn=policy_arn)
    print('The role policy \'{}\' is attached to IAM role \'{}\' '.format(policy_arn, iam_role_name))

def launch_redshift_cluster(redshift_client, cluster_identifier,
                            cluster_type, node_type, num_nodes,
                            db_name, db_user, db_password, iam_role_arn):
    """ Launches a new Redshift cluster with provided cluster properties.
    
    Args:
    redshift_client (boto3 Redshift client)
    cluster_identifier (str): Cluster identifier
    cluster_type (str): Type of cluster
    node_type (str): multi-node / single-node
    num_nodes (int): number of nodes
    db_name (str): database name
    db_user (str): master username
    db_password (str): master password
    iam_role_arn (str): IAM role for redshift cluster
    """
    if cluster_type == 'single-node':
        response = redshift_client.create_cluster(        
            # add parameters for hardware
            ClusterType=cluster_type,
            NodeType=node_type,
            # add parameters for identifiers & credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            # add parameter for IAM role
            IamRoles=[iam_role_arn])
    else:
        response = redshift_client.create_cluster(        
            # add parameters for hardware
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            # add parameters for identifiers & credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            # add parameter for IAM role
            IamRoles=[iam_role_arn])
    print('The request for launching Redshift cluster is successfully submitted')

def wait_for_cluster_status_available(redshift_client, cluster_identifier, timeout=10.0):
    """ Checks and waits for Redshift cluster to become available until timeout.
    
    Args:
    redshift_client (boto3 Redshift client)
    cluster_identifier (str): Cluster identifier
    timeout (float): timeout value in minutes 
    """
    sleep_period = 2
    timeout_seconds = timeout*60
    time_start = time.time()
    time_pass = time.time() - time_start
    while time_pass < timeout_seconds:
        try:
            cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            if cluster_props['ClusterStatus'] == 'available':
                print('Time passed: {:05.1f} seconds, Redshift cluster is now available for use'.format(time_pass))
                return cluster_props
            else:
                print('Time passed: {:05.1f} seconds, Redshift cluster status: \'{}\''.format(time_pass, cluster_props['ClusterStatus']))
        except redshift_client.exceptions.ClusterNotFoundFault:
            print('Time passed: {:05.1f} seconds, Redshift cluster is not found yet ...'.format(time_pass))
        time.sleep(sleep_period)
        time_pass = time.time() - time_start
    raise Exception('WARNING: Redshift cluster did not become available, please delete it on AWS web console and try again.')

def create_cluster(config, ec2_resource, iam_client, redshift_client):
    """ Creates the IAM role, launches Redshift Cluster and opens an incoming TCP access port.
    
    Args:
    config: configuration for AWS/S3/Redshift cluster/IAM role
    ec2_resource (boto3 EC2 resource)
    iam_client (boto3 IAM client)
    redshift_client (boto3 Redshift client)
    """
    # Get IAM role name and its policy
    iam_role_name = config.get('IAM_ROLE','role_name')
    policy_arn = config.get('IAM_ROLE','policy_arn')
    # create IAM role for redshift to provide S3 read only access
    try:
        create_iam_role_with_policy(iam_client, iam_role_name, policy_arn)
    except Exception as e:
        print('IAM role (RoleName={}) could not be created\n{}'.format(iam_role_name, e))
        return
    # Update the IAM role ARN in the config file
    iam_role_arn = iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']
    config.set('IAM_ROLE', 'arn', "'{}'".format(iam_role_arn))

    # Get Cluster/Database properties
    cluster_type = config.get('CLUSTER_PROP','cp_cluster_type')
    node_type = config.get('CLUSTER_PROP','cp_node_type')
    num_nodes = config.get('CLUSTER_PROP','cp_num_nodes')
    cluster_identifier = config.get('CLUSTER_PROP','cp_cluster_identifier')
    db_name = config.get('CLUSTER','db_name')
    db_user = config.get('CLUSTER','db_user')
    db_password = config.get('CLUSTER','db_password')
    # Launch Redshift cluster
    try:
        launch_redshift_cluster(redshift_client, cluster_identifier,
                                cluster_type, node_type, int(num_nodes),
                                db_name, db_user, db_password, iam_role_arn)
    except Exception as e:
        print(e)
        return
    # Wait Redshift cluster
    try:
        cluster_props = wait_for_cluster_status_available(redshift_client, cluster_identifier)
    except Exception as e:
        print(e)
        return

    # Update clsuter host in the config file
    db_host = cluster_props['Endpoint']['Address']
    config.set('CLUSTER', 'host', db_host)
    print('The cluster endpoint adress: {}'.format(db_host))
    # Save the update config file for later use
    with open(config_filename, 'w') as configfile:
        config.write(configfile)

    # Open an incoming TCP port to access the cluster endpoint
    print('Creating an incoming TCP port to access the cluster endpoint...')
    db_port = config.get('CLUSTER','db_port')
    try:
        vpc = ec2_resource.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0', 
            IpProtocol='TCP',
            FromPort=int(db_port),
            ToPort=int(db_port))
    except Exception as e:
        if 'InvalidPermission.Duplicate' in str(e):
            print('TCP port access rule already exists for the default security group')
        else:
            print(e)
            return
    print('Redshift cluster setup is now completed succesfully and ready for use')

def delete_cluster(config, redshift_client):
    """ Deletes Redshift cluster with given identifier.
    
    Args:
    config: configuration for AWS/S3/Redshift cluster/IAM role
    redshift_client (boto3 Redshift client)
    """
    # Get the cluster idenfier
    cluster_identifier = config.get('CLUSTER_PROP','cp_cluster_identifier')

    # Delete Redshift cluster if it exists
    try:
        cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        print('Redshift cluster with identifier \'{}\' is found with status \'{}\', will delete it'.format(cluster_identifier, cluster_props['ClusterStatus']))
        # Submit a request to delete the cluster
        redshift_client.delete_cluster(ClusterIdentifier=cluster_identifier,  SkipFinalClusterSnapshot=True)
        print('The request for Redshift cluster deletion is successfully submitted')
    except redshift_client.exceptions.ClusterNotFoundFault:
        print('Redshift cluster with identifier \'{}\' is not found, cannot delete it'.format(cluster_identifier))
        return
    except Exception as e:
        print(e)
        return

    # Check cluster status until it is deleted
    deleted = False
    sleep_period = 2
    time_start = time.time()
    time_pass = time.time() - time_start
    while deleted == False:
        try:
            cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            print('Time passed: {:05.1f} seconds, status of the Redshift cluster: \'{}\''.format(time_pass, cluster_props['ClusterStatus']))
        except redshift_client.exceptions.ClusterNotFoundFault:
            print('Time passed: {:05.1f} seconds, the redshift cluster is now successfully deleted'.format(time_pass))
            deleted = True
        except Exception as e:
            print(e)
            return
        time.sleep(sleep_period)
        time_pass = time.time() - time_start

def describe_cluster(config, redshift_client):
    """ Checks and reports cluster status and its endpoint adress
    
    Args:
    config: configuration for AWS/S3/Redshift cluster/IAM role
    redshift_client (boto3 Redshift client)
    """
    # Get Redshift cluster identifier
    cluster_identifier = config.get('CLUSTER_PROP','cp_cluster_identifier')
    try:
        cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        print('Redshift cluster with identifier \'{}\' is found with status \'{}\''.format(cluster_identifier, cluster_props['ClusterStatus']))
        cluster_endpoint = cluster_props.get('Endpoint')
        if cluster_endpoint is not None:
            db_host = cluster_endpoint.get('Address')
            if db_host is not None:
                print('The cluster endpoint adress: {}'.format(db_host))
    except redshift_client.exceptions.ClusterNotFoundFault:
        print('Redshift cluster with identifier \'{}\' is not found, cannot describe it'.format(cluster_identifier))
    except Exception as e:
        print(e)

def main(action):
    """ Performs the selected action using boto3 library
    
    Args:
    action (str): options are 'create', 'delete', 'describe'
    """
    # Parse the configuratin file
    config = configparser.ConfigParser()
    config.read(config_filename)

    # Get AWS access credentials
    access_key_id = config.get('AWS','KEY')
    secret_access_key = config.get('AWS','SECRET')
    region_name = config.get('AWS','REGION')
    # create boto3 clients required for cluster setup
    try:
        ec2_resource, iam_client, redshift_client = create_clients(access_key_id, secret_access_key, region_name)
    except Exception as e:
        print('AWS boto3 clients could not be initialized !\n{}'.format(e))
        return

    if action == 'create':
        create_cluster(config, ec2_resource, iam_client, redshift_client)
    elif action == 'delete':
        delete_cluster(config, redshift_client)
    elif action == 'describe':
        describe_cluster(config, redshift_client)

if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="A python script to manage AWS Redshift Clusters through SDKs", add_help=True)
    parser.add_argument("action", type=str, choices=['create', 'delete', 'describe'],
                        help="""create: creates the Redshift cluster,
                                delete: deletes the Redshift cluster,
                                describe: reports status of the Redshift cluster
                             """)
    args = parser.parse_args()
    # Configuration filename
    config_filename = 'dwh.cfg'
    # Call main function
    main(args.action)
    
    
    