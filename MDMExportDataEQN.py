from sqlalchemy import create_engine
import urllib
import sqlalchemy
import pandas as pd
import sys
from datetime import datetime, timedelta
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


def getQuestionDesc(cols_old_list):
    cols_new_list = []
    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.110;Database=APQuestionnaire;uid=sa;pwd=sql@apthai;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    str_sql = """
        SELECT id, Question FROM dbo.Question ORDER BY id
    """
    # Read by SQL Statement
    df_mst_qn = pd.read_sql(sql=str_sql, con=db)

    for idx, val in enumerate(cols_old_list):
        # print(idx, val)
        if(str(val).isdigit()):
            df0 = df_mst_qn.loc[lambda df: df.id == int(val)]
            val = df0.iat[0, 1]
            cols_new_list.append(val)
        else:
            cols_new_list.append(val)

    # print(cols_new_list)
    return cols_new_list


def main():
    # python3 MDMExportDataEQN.py <StartDate> <EndDate>
    if len(sys.argv) == 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
        end_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

    # print(start_date, end_date)

    params = 'Driver={ODBC Driver 17 for SQL Server};Server=192.168.2.110;Database=APQuestionnaire;uid=sa;pwd=sql@apthai;'
    params = urllib.parse.quote_plus(params)

    db = create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, fast_executemany=True)

    previous_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    # print(previous_date)

    str_sql = """
        EXEC dbo.spGetQuestionnaireAnswerV2ForExport_forKai @VisitDateBegin = '{}',
                                                    @VisitDateEnd = '{}',         
                                                    @QuestionnaireTypeId = 1,    
                                                    @BUIds = N'',                
                                                    @BrandIds = N'',             
                                                    @QuestionnaireIds = N'',     
                                                    @IncludeIsBooking = 0,       
                                                    @AddressDistrictIds = N'',   
                                                    @IncludeInActiveProject = 0, 
                                                    @UserID = 0,
                                                    @PivotFlag = 'Y'
    """.format(start_date, end_date)

    # Setup Format File Name CSV
    # date_fmt = datetime.now().strftime("%Y%m%d")
    date_fmt = datetime.now().strftime("%Y%m%dT%H%M%S-01")
    file_name = "EQuestionnaire_QuestionnaireSummary_"
    file_type = ".csv"
    full_file_name = "/home/ubuntu/myPython/mdm/csvfile/{}{}{}".format(file_name, date_fmt, file_type)
    logging.info("File Name => {}".format(full_file_name))

    df = pd.read_sql(sql=str_sql, con=db)

    # cols_new_list = getQuestionDesc(df.columns.tolist())
    df.columns = getQuestionDesc(df.columns.tolist())

    # Read by SQL Statement
    # pd.read_sql(sql=str_sql, con=db).to_csv(full_file_name, index=False, encoding="utf-8")
    logging.info("<<<Before Read SQL to CSV File>>>")
    df.to_csv(full_file_name, index=False, encoding="utf-8")
    logging.info("<<<After Read SQL to CSV File>>>")
    
    # Upload file to AWS S3    
    s3_file = 'Landing/E-Questionnaire/{}{}{}'.format(file_name, date_fmt, file_type)
    
    uploaded = upload_to_aws(full_file_name, BUCKET_NAME, s3_file)


if __name__ == '__main__':
    logFile = r"/home/ubuntu/myPython/mdm/log/MDMExportDataEQN.log"

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
