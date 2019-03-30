
# coding: utf-8

#import jira
from jira import JIRA
import pyodbc
import sqlalchemy
import getpass
import datetime
import pandas as pd
import numpy as np
import os
import time
import email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import smtplib
import xlsxwriter
import argparse
from os.path import expanduser
import json
import pytz

__filename__ = "Bizops Control Center - Execution"
def tz2ntz(date_obj, tz, ntz):
    
    # date_obj: datetime object
    # tz: old timezone
    # ntz: new timezone
    
    if isinstance(date_obj, datetime.date) and tz and ntz:
       date_obj = date_obj.replace(tzinfo=pytz.timezone(tz))
       return date_obj.astimezone(pytz.timezone(ntz))
    return False

def jira_reporting_services(parameters, notify):
    print("Welcome to JRS")
    engine = sqlalchemy.create_engine("mssql+pyodbc://" + str(db_username) + ":" + str(db_password) + "@" + str(db_server) + ":" + str(db_port) + "/" + str(db_database) + "?driver=" + str(db_driver))
    print(engine)
    log = []
    for i in parameters.index:
        attempt = 'Pass'
        utc_now = datetime.datetime.utcnow()
        pst_now = tz2ntz(utc_now, 'UTC', 'US/Pacific') 
        now = pst_now.strftime('%Y-%m-%d') 
        execution_time = str(pst_now.strftime('%Y-%m-%d %H:%M:%S'))   
        start = time.time()
        # Try and Except block to exempt failed audit runs
        try:
            # Gathering Query from Source
            print('\n' + color_start + str(parameters['Audit_Number'][i]) + ': ' + str(parameters['Query_Name'][i]) + color_end)
            print("Running Audit")
            query_path = queryPath + str(parameters['Query_Name'][i]) + '.sql'
            query = " ".join(open(query_path, 'r').readlines())
            print("Query Established")
            print("Connecting to Database")
            cnxn=engine.connect()
            cnxn.execution_options(autocommit = True, autoflush = False, expire_on_commit = False)
            print("Connected to Database")
            print("Executing Audit. Reading Data into Pandas Dataframe.")
            df = pd.read_sql_query(query, cnxn)
            end = time.time()
            print("Data Read into Pandas Dataframe")
            duration = "%0.2f" % (end-start)
            print("Closing Connection to Database")
            cnxn.close()
            print(str(parameters['Query_Name'][i]) + ' Successfully Executed in ' + duration + ' seconds.\n')
            if len(df.index) > 0:
                if notify == 'TRUE':
                    # Creating Jira Ticket if Audit Found Data
                    print("Audit: " + str(parameters['Query_Name'][i]).replace("'","") + ", Found Data. Creating Jira Ticket")
                    ticket_fields = {
                        'project': {'id': str(int(parameters['Project'][i])).replace("'","")},
                        'summary': str(parameters['Query_Name'][i]).replace("'","") + ' %s' % now,
                        'issuetype': {'name' : str(parameters['Issue_Type'][i]).replace("'","")},
                        'priority': {'name': str(parameters['Priority'][i]).replace("'","")},
                        'description': str(parameters['Description'][i]).replace("'","")
                    }
                    ticket = jira.create_issue(fields = ticket_fields)
                    # Assigning ticket to Assignee in Parameters
                    if parameters['Assignee'][i] != None:
                        ticket.update(assignee = {'name': str(parameters['Assignee'][i]).replace("'","")})
                    else:
                        print('Assignee for Ticket Not Provided.')
                    # Adding Comment specified in Parameters
                    if parameters['Comment'][i] != None:
                        jira.add_comment(ticket, str(parameters['Comment'][i]).replace(" ",""))
                    else:
                        print('Comment for Ticket Not Provided.')
                    # Adding Audit Results as a CSV attachment
                    print("Attaching Data for Current Audit Ticket")
                    data_path = dataPath + str(parameters['Query_Name'][i]) + ' Audit Data ' + str(now) + '.csv'
                    df.to_csv(data_path)
                    time.sleep(1)
                    jira.add_attachment(ticket,data_path)
                    time.sleep(1)
                    jira_link = ticket.permalink()
                    list = str.split(jira_link, '/')
                    jira_number = str(list[4])

                else:
                    print ("There is no notifications ON for this run")
                    jira_number = ''
            else:
                print("No Data found for Audit: " + str(parameters['Query_Name'][i]).replace("'","") + ". Moving to next audit.")
                jira_number = ''

            log.append({'Audit_Number': int(parameters['Audit_Number'][i])
                       ,'Audit_Name': str(parameters['Query_Name'][i])
                       ,'Number_of_Results': len(df.index)
                       ,'Execution_Time': execution_time
                       ,'Duration': duration
                       ,'Date': now
                       ,'Attempt': attempt
                       ,'Info':''
                       ,'Jira_Ticket': jira_number})
        except Exception as error:
            print('Failed to execute SQL query. Please make sure to follow guidelines related to hosting audits in Control Center.')
            # Closing Database connection if Query fails to Execute
            try:
                cnxn.close()
                print('Connection Closed after Audit Query Failed to Execute')
            except Exception as e:
                print('No Database Connection to Close') 
                print(e)
            end_fail = time.time()
            duration_fail = "%0.2f" % (end_fail-start)
            print("Time duration for this audit: {}".format(duration_fail))
            print(error)
            attempt = 'Fail'

            log.append({'Audit_Number': int(parameters['Audit_Number'][i])
                       ,'Audit_Name': str(parameters['Query_Name'][i])
                       ,'Number_of_Results': -1
                       ,'Execution_Time': execution_time
                       ,'Duration': duration_fail
                       ,'Date': now
                       ,'Attempt': attempt
                       ,'Info': error
                       ,'Jira_Ticket': ''})

    print("Execution of Audits Complete.")
    return log


def logToDB(audit_log):
    engine = sqlalchemy.create_engine("mssql+pyodbc://" + str(db_username) + ":" + str(db_password) + "@" + str(db_server) + ":" + str(db_port) + "/" + str(db_database) + "?driver=" + str(db_driver))
    print(engine)
    boolIdx = audit_log['Info'].notnull()
    audit_log['Info']=audit_log['Info'].astype(str)
    audit_log.ix[boolIdx, "Info"] = audit_log.ix[boolIdx, "Info"].apply(lambda x: x[:250])
    if len(audit_log) > 0:
        try:
            conn = engine.connect()
            time.sleep(2)
            audit_log.to_sql('ControlCenterLog', conn, if_exists='append', index= False)
            time.sleep(2)
            conn.close()
        except Exception as error:
            print('Error in Logging', error)
        conn = engine.connect()
        dt = audit_log['Execution_Time'].min()
        df = pd.read_sql("select * from BizOpsAudit.dbo.ControlCenterLog where Execution_Time >= '{}'".format(dt), conn)
        conn.close()
    else:
        df = pd.dataframe()
        print('No audits found to be executed. Logging not required.')
    return df

if __name__ == '__main__':
    try:
        utc_now = datetime.datetime.utcnow()
        pst_now = tz2ntz(utc_now, 'UTC', 'US/Pacific')
        print('Pacific Time: ' + str(pst_now))
        dtStart = pst_now
        dtStart = dtStart.replace(second=0,microsecond=0)
        dtStartIso = dtStart.isoformat(sep=" ")
        print("The script {} starts executing now ".format(__filename__) + str(dtStartIso))
        #Get todays day to see which audits are to be scheduled
        day_of_week = pst_now.strftime('%A')
        print("Audits should be run for today",day_of_week)
        now = pst_now.strftime('%Y-%m-%d')
        current_time = pst_now.strftime('%H.%M')
        previous_half_hour = pst_now - datetime.timedelta(hours = 1)
        color_start = "\033[1m"
        color_end = "\033[0;0m"
        
        db_username = os.environ.get("CIRCLEONE_DB_USERNAME",'')
        db_password = os.environ.get("CIRCLEONE_DB_PASSWORD",'')
        db_server = os.environ.get("CIRCLEONE_DB_SERVER",'')
        db_port = os.environ.get("CIRCLEONE_DB_PORT",'')
        db_database = os.environ.get("CIRCLEONE_DB_NAME",'')
        notify = os.environ.get("NOTIFICATION") # 'False' or 'True'
        logging = os.environ.get("LOGGING") # 'False' or 'True'
        db_driver = "ODBC+Driver+17+for+SQL+Server"
        jira_server = os.environ.get("JIRA_SERVER")
        jira_username = os.environ.get("JIRA_USERNAME")
        jira_password = os.environ.get("JIRA_PASSWORD")
        queryPath = '/app/Control Center Queries/'
        dataPath = '/app/Data Attachments/'

        engine = sqlalchemy.create_engine("mssql+pyodbc://" + str(db_username) + ":" + str(db_password) + "@" + str(db_server) + ":" + str(db_port) + "/" + str(db_database) + "?driver=" + str(db_driver))
        conn=engine.connect()
        df = pd.read_sql("select * from BizOpsAudit.dbo.ControlCenterLog where date = '{}'".format(now), conn)
        conn.close()
        time.sleep(2)
        #Get the entire list of Audit schedule table
        conn = engine.connect()
        report=pd.read_sql("select * from BizOpsAudit.dbo.ControlCenterSchedule", conn)
        conn.close()
        report = report.where((pd.notnull(report)), None)
        report = report[report[day_of_week] == 1]
        #Check the audits that were supposed to run until Now
        report = report[report['Hour_Run'] <= float(current_time)]
        print ("Total number of audits till now that had to be run",len(report))

        # Determine which audits have run/not run based on the log data for today
        report = report[~report['Audit_Number'].isin(list(df['Audit_Number']))]
        print ("Audits to be run in this schedule",len(report))

        if len(report) > 0:
            jira = JIRA(options = {"server":jira_server} , basic_auth = (jira_username, jira_password))
            print("Audits going through Jira Reporting Services")
            log = jira_reporting_services(report, notify)
            
            if len(log) > 0:
                try:
                    first_log = pd.DataFrame(log, index = None)
                    print(first_log.info())
                    failed_audits = first_log[first_log['Attempt'] == 'Fail']
                    second_run = len(failed_audits)
                    print ("There are {} failed audits to be run now".format(second_run))
                    if second_run > 0:
                        secondrun_df = report.loc[report['Query_Name'].isin(failed_audits.Audit_Name)]
                        print("Second Run of Failed Audits, going through Jira Reporting Parameters.")
                        second_log = jira_reporting_services(secondrun_df, notify)
                        second_log = pd.DataFrame(second_log, index = None)
                        audit_log = first_log[first_log.Attempt == 'Pass']
                        audit_log = audit_log.append(second_log)
                        print(audit_log.info())
                    else:
                        audit_log = first_log.copy()
                        print('No failed audits to re-run.')
                    if logging == 'TRUE':
                        print ("Audit entries needs to be logged in DB")
                        df = logToDB(audit_log)
                        print ("There are {} audits that have been logged in this run".format(len(df)))
                    else:
                        print ("There is nothing to be logged in the DB table")
                except Exception as error:
                    print("Unable to log entries to DB table")
                    raise error
            else:
                print ("There are no audits found to be logged")
        else:
            print ("There are no audits in this scheduled run")
        utcEnd = datetime.datetime.utcnow()
        dtEnd = tz2ntz(utcEnd, 'UTC', 'US/Pacific') 
        dtEnd = dtEnd.replace(second=0,microsecond=0)
        dtEndIso = dtEnd.isoformat(sep=" ")
        print("The script finished execution at {}".format(dtEnd))
        print("Total time taken for the script to execute {} ".format(dtEnd - dtStart))

    except Exception as error:
        print(error)
        fromaddr = 'BizOpsControls@prosper.com'
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        
        if notify == 'FALSE':
            toaddr = ['nseelam@prosper.com','mcordon@prosper.com','akundurthy@prosper.com']
            msg['Subject'] = "STG FAILURE: Control Center Execution"
        else:
            toaddr = ['nseelam@prosper.com','mcordon@prosper.com','akundurthy@prosper.com', 'ksun@prosper.com','cweber@prosper.com','pdosanjh@prosper.com']
            msg['Subject'] = "PROD FAILURE: Control Center Execution"
        
        msg['To'] = ", ".join(toaddr)
        html = """\
        <html>
          <head></head>
          <font face="Verdana">
          <body>
            <p>Hello Team,<br><br>
               Control Center Failed to Execute Properly. Please investigate the issue.<br><br>
               Sincerely,<br>
               Control Center<br>
            </p>
          </body>
          </font>
        </html>
        """
        msg.attach(MIMEText(html,'html'))

        server = smtplib.SMTP('10.209.32.22')
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
