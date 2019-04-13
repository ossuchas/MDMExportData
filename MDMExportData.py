from sqlalchemy import create_engine
import urllib
import sqlalchemy
import pandas as pd
import sys
from datetime import datetime
import boto3.session
from botocore.exceptions import NoCredentialsError
import logging


# BUCKET_NAME = 'apthai-stgbucket0'
# PROFILE_NAME = 'myaws2'

BUCKET_NAME = 'apthai-stgbucket'
PROFILE_NAME = 'mdm'


def upload_to_aws(local_file, bucket, s3_file):
    dev = boto3.session.Session(profile_name=PROFILE_NAME)
 
    s3 = dev.resource('s3')

    try:
        s3.meta.client.upload_file(local_file, bucket, s3_file)
        logging.info("Upload File to AWS S3Successful")
        return True
    except FileNotFoundError:
        logging.error("The file was not found")
        return False
    except NoCredentialsError:
        logging.error("Credentials not available")
        return False


def main():
    # python3 MDMExportData.py <SYSTEM> <TABLENAME> <SPLITFLAG> <NUMBEROFRECORD>
    system = sys.argv[1]
    tbl_name = sys.argv[2]
    split_flag = sys.argv[3]

    c_top_flag = 'N'
    if len(sys.argv) == 5:
        sel_top = sys.argv[4]
        c_top_flag = 'Y'

    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.58;Database=db_iconcrm_fusion;uid=iconuser;pwd=P@ssw0rd;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    str_sql = """
        SELECT SelectColumn + ' ' + FromColumn + ' ' +  WhereColumn + ' ' + ISNULL(ConditionColumn,'') as SQLStatement, 
                FileName, FileType
        FROM dbo.MST_MDMTableList
        WHERE System = '{}'
        AND TableName = '{}'
        AND SeqnSplitNo = {}
    """.format(system, tbl_name, split_flag)

    df = pd.read_sql(sql=str_sql, con=db)

    # Setup Format File Name CSV
    # date_fmt = datetime.now().strftime("%Y%m%d")
    date_fmt = datetime.now().strftime("%Y%m%dT%H%M%S-01")
    file_name = df.iat[0, 1]
    file_type = df.iat[0, 2]
    full_file_name = "/home/ubuntu/myPython/mdm/csvfile/{}{}{}".format(file_name, date_fmt, file_type)
    logging.info("File Name => {}".format(full_file_name))

    # Setup format String SQL Statement
    str_sql = df.iat[0, 0]

    if c_top_flag == 'Y':
        _sel = 'SELECT TOP {} '.format(sel_top)
        str_sql = str_sql.replace('SELECT ', _sel)

    # Read by SQL Statement
    logging.info("<<<Before Read SQL to CSV File>>>")
    pd.read_sql(sql=str_sql, con=db).to_csv(full_file_name, index=False, encoding="utf-8")
    logging.info("<<<After Read SQL to CSV File>>>")
    
    # Upload file to AWS S3    
    s3_file = 'Landing/CRM1-2/{}{}{}'.format(file_name, date_fmt, file_type)
    
    uploaded = upload_to_aws(full_file_name, BUCKET_NAME, s3_file)


if __name__ == '__main__':
    tbl_name = sys.argv[2]
    logFile = r"/home/ubuntu/myPython/mdm/log/MDMExportData_{}.log".format(tbl_name)

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)-5s [%(levelname)-8s] >> %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=logFile,
                        filemode='a')

    logging.debug('#####################')
    logging.info('Start Process')
    main()
    logging.info('End Process')
    logging.debug('#####################')
