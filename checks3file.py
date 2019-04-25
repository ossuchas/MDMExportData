# import boto3
import boto3.session 
from botocore.exceptions import NoCredentialsError
import sys


# BUCKET_NAME = 'apthai-stgbucket0'
BUCKET_NAME = 'apthai-stgbucket'


def upload_to_aws(local_file, bucket, s3_file):
    dev = boto3.session.Session(profile_name='myaws2')
    # dev = boto3.session.Session(profile_name='mdm')
 
    s3 = dev.resource('s3')

    try:
        s3.meta.client.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def upload_to_aws_mdm(local_file, bucket, s3_file):
    dev = boto3.session.Session(profile_name='mdm')
 
    s3 = dev.resource('s3')

    try:
        s3.meta.client.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


def delete_file_aws(bucket, s3_file):
    dev = boto3.session.Session(profile_name='myaws2')
 
    s3 = dev.resource('s3')
    
    obj = s3.Object("apthai-stgbucket0", "Landing/hello.txt")
    obj.delete()
    print("Delete Success...!!")
    return True


def list_file_aws(bucket, _prefix):
    dev = boto3.session.Session(profile_name='myaws2')
 
    s3 = dev.resource('s3')

    for bucket in s3.Bucket('apthai-stgbucket0').objects.filter(Prefix=_prefix):
        print(bucket.key)


def list_file_aws_mdm(bucket, _prefix):
    dev = boto3.session.Session(profile_name='mdm')
 
    s3 = dev.resource('s3')

    for bucket in s3.Bucket('apthai-stgbucket').objects.filter(Prefix=_prefix):
        print(bucket.key)


def main():

    parm_path = sys.argv[1]

    s3_path = r"Landing/{}".format(parm_path)
    print(s3_path)

    local_file = 'hello.txt'
    #s3_file = 'Landing/CRM1-2/hello.txt'
    s3_file = 'Landing/BC/Book/hello.txt'
    # s3_file = 'Landing/CRM1-2/ActivityLead/hello.txt'
    
    # uploaded = upload_to_aws(local_file, BUCKET_NAME, s3_file)
    uploaded = upload_to_aws_mdm(local_file, BUCKET_NAME, s3_file)
    # list_file_aws_mdm(BUCKET_NAME, 'Landing/CRM1-2/Booking')
    # list_file_aws_mdm(BUCKET_NAME, 'Landing/CRM1-2')
    # list_file_aws_mdm(BUCKET_NAME, 'Landing/LMS')
    list_file_aws_mdm(BUCKET_NAME, s3_path)
    # list_file_aws(BUCKET_NAME, 'Landing/CRM1-2')
    
    # deleted = delete_file_aws(s3_file, BUCKET_NAME)
    # list_file_aws(BUCKET_NAME, 'Landing/')


if __name__ == '__main__':
    main()
