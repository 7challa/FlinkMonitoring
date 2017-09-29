#!/usr/bin/python

from __future__ import print_function
import requests
import smtplib
import sys

#Flag to enable/disable email notification
sendEmailNotification=True
SMTP_HOST='smtp_server_comes_here'

#Cloudera Manager URL
CM_URL = 'cm_url_comes_here'

#Resource Manager URLs - Assuming HA here
# If no HA, RM1_URL = RM2_URL
RM1_URL= 'resource_manager1_url_comes_here'
RM2_URL= 'resource_manager2_url_comes_here'

#Cloudera Cluster Name
CLUSTER_NAME = 'cloudra_cluster_name'


# Email sender
sender = 'sender_email_address'

# List of email receivers
receivers = ['recepient_email_comes_here']


def sendEmailNotification(SUBJECT,TEXT):
    # print('Subject: ', SUBJECT)

    BODY = '\r\n'.join([
            'To: %s' % ','.join(receivers),
            'From: %s' % sender,
            'Subject: %s' %SUBJECT,
            '',
            TEXT
            ])
    try:
        smtpObj = smtplib.SMTP(SMTP_HOST)
        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.sendmail(sender, receivers, BODY)
        print('Successfully sent email')
    except:
        print('sendEmailNotification Error: unable to send email')
    finally:
        smtpObj.quit()

def getYarnAppId():
    try:
        URL = CM_URL + '/api/v10/clusters/' + CLUSTER_NAME  + '/services/yarn/yarnApplications'
        print('URL: ', URL)
        resp = requests.get(URL, auth=('monitor', 'monitor'))
        #print(resp.json().keys())

    except:
        sendEmailNotification('Critical: Unable to access Flink/Kafka Cloudera Manager',
                              'Unable to get to Cloudera Manager to monitor Flink Yarn App - Please check')

    else:
        print('YarnApplication on the cluster: ', resp.json()['applications'])
        if len(resp.json()['applications']) == 0:
            sendEmailNotification('Critical: No Yarn Application Found Event Filter',
                                  'Unable to Flink Yarn App - Please check')
            sys.exit()
        else:
            application_ID = resp.json()['applications'][0]['applicationId']
            print('YarnApplicationID:', resp.json()['applications'][0]['applicationId'])
            return application_ID
        
def findFlinkURL():
    yarnAppId=getYarnAppId()

    FLINK_URL_1 = RM1_URL + '/proxy/' + yarnAppId + '/jobs/'
    FLINK_URL_2 = RM2_URL + '/proxy/' + yarnAppId + '/jobs/'

    try:
        if "application/json" in requests.get(FLINK_URL_1).headers['content-type']:
            return FLINK_URL_1
        else:
            return FLINK_URL_2
    except:
        event_subject = 'Flink could be down or not responding'
        event_body = 'Unable to get to /jobs endpoint to mointor Flink - Please check'
        sendEmailNotification(event_subject,event_body)

# print('Flink URL: {} '.format(findFlinkURL()))




def monitorFlink():

    FLINK_URL=findFlinkURL()

    print('Flink URL: ', FLINK_URL)
    try:
        resp = requests.get(FLINK_URL)
        print(resp.json().keys())
        print('Jobs Running:', resp.json()['jobs-running'])
        print('Jobs Failed:', resp.json()['jobs-failed'])
        print('Jobs Cancelled:', resp.json()['jobs-cancelled'])

        #Send email if there are no running jobs
        if len(resp.json()['jobs-running']) < 1:
            print("Critical: Event Filter Flink Job is DOWN!")
            event_subject = 'Critical: Event Filter Flink Job is DOWN!'
            event_body = 'No running event filter job found. Please check the logs for more details.'
            if sendEmailNotification:
                sendEmailNotification(event_subject, event_body)

        #Send email if there is more than one running Job
        if len(resp.json()['jobs-running']) > 1:
            print("More than ONE Flink Job is RUNNING - Please check")
            event_subject = 'ERROR: More than One Flink Job is RUNNING!'
            event_body = 'More than One Flink Job is running. Please check.'
            if sendEmailNotification:
                sendEmailNotification(event_subject, event_body)

        #Send email if there are failed jobs
        if len(resp.json()['jobs-failed']) > 0:
            print("Filnk Job Failed")
            event_subject = 'Event Filter Job Failed!'
            event_body = 'Event Filter job failed. Please check the logs for more details.'
            if sendEmailNotification:
                sendEmailNotification(event_subject, event_body)
    except:
        event_subject = 'Flink could be down or not responding'
        event_body = 'Unable to get to /jobs endpoint to mointor Flink - Please check'
        sendEmailNotification(event_subject,event_body)

def main():
    monitorFlink()

if __name__ == "__main__":
    main()
