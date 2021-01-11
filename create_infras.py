import pandas as pd
import boto3
import json


# Load DWH Params from a file
import configparser
config_infra = configparser.ConfigParser()
config_infra.read_file(open('infra.cfg'))

KEY                    = config_infra.get('AWS','KEY')
SECRET                 = config_infra.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config_infra.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config_infra.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config_infra.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config_infra.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config_infra.get("DWH","DWH_DB")
DWH_DB_USER            = config_infra.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config_infra.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config_infra.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config_infra.get("DWH", "DWH_IAM_ROLE_NAME")

print(pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             }))

###


# Create clients for IAM, S3 and Redshift
import boto3

s3 = boto3.resource("s3", region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)

iam = boto3.client("iam", region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)

redshift = boto3.client("redshift", region_name = "us-west-2", aws_access_key_id = KEY, aws_secret_access_key = SECRET)


# Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
# Create the IAM role
try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(Path="/",
                             RoleName=DWH_IAM_ROLE_NAME,
                             Description="Allows Redshift Clusters to call AWS services on your behalf.",
                             AssumeRolePolicyDocument = json.dumps(
                             {
                                 "Statement":[{"Action":"sts:AssumeRole",
                                              "Effect":"Allow",
                                              "Principal":{"Service":"redshift.amazonaws.com"}}],
                                 "Version":"2012-10-17"
                             }))
    

except Exception as e:
    print(e)
    
    
# Attach Policy
print('1.2 Attaching Policy')
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME, 
                      PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")


# Get and print the IAM role ARN
print('1.3 Get the IAM role ARN')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)["Role"]["Arn"]

print('ARN : ', roleArn)

###


# Create a RedShift Cluster
try:
    response = redshift.create_cluster(        
        # TODO: add parameters for hardware
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),
        

        # TODO: add parameters for identifiers & credentials
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        
        # TODO: add parameter for role (to allow s3 access)
        IamRoles = [roleArn]
    )

except Exception as e:
    print(e)

###


# Describe the cluster to see its status
import itertools
import threading
import time
import sys

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

done = False
#here is the animation
def animate():
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if done:
            break
        sys.stdout.write('\rcreating ' + c )
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\ravailable!     '+'\n')

t = threading.Thread(target=animate)
t.start()

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
while myClusterProps['ClusterStatus'] != 'available':
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    time.sleep(1)

print()
print(prettyRedshiftProps(myClusterProps))

done = True
t.join()

###

# Defined endpoint and arn
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

###


# Config dwh.cfg
config_dwh = configparser.ConfigParser()
config_dwh.read_file(open('dwh.cfg'))

#Assume we need 2 sections in the config file, let's call them USERINFO and SERVERCONFIG
config_dwh["CLUSTER"] = {
    "HOST": DWH_ENDPOINT,
    "DB_NAME": DWH_DB,
    "DB_USER": DWH_DB_USER,
    "DB_PASSWORD": DWH_DB_PASSWORD,
    "DB_PORT": DWH_PORT
}

config_dwh["IAM_ROLE"] = {
    "ARN": DWH_ROLE_ARN
}

config_dwh["S3"] = {
    "LOG_DATA": config_dwh.get('S3', 'LOG_DATA'),
    "LOG_JSONPATH": config_dwh.get('S3', 'LOG_JSONPATH'),
    "SONG_DATA": config_dwh.get('S3', 'SONG_DATA')
}


#Write the above sections to config.ini file
with open('dwh.cfg', 'w') as conf:
    config_dwh.write(conf)
    
###