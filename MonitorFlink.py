#!/usr/bin/python

from __future__ import print_function
import requests
import smtplib
import sys

# Flag to enable/disable email notification
sendEmailNotification = True
SMTP_HOST='10.10.10.10'


### Stage
CM_URL = 'http://cloudera_manager_host:<cloudera_port>'
CLUSTER_NAME = 'NAME_OF_THE_CLUSTER'
# CM API VERSION
API_VERSION = 'v19'


# Email sender
sender = 'Flink@sender.com'

# List of email receivers
receivers = ['email@email.com']


def sendEmailNotification(SUBJECT, TEXT):
    # print('Subject: ', SUBJECT)

    BODY = '\r\n'.join([
        'To: %s' % ','.join(receivers),
        'From: %s' % sender,
        'Subject: %s' % SUBJECT,
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
        URL = CM_URL + '/api/' + API_VERSION +  '/clusters/' + CLUSTER_NAME + '/services/yarn/yarnApplications'
        print('YARN_APP_URL: ', URL)
        resp = requests.get(URL, auth=('monitor', 'monitor'))
        # print(resp.json().keys())

    except:
        sendEmailNotification('Critical: Unable to access Flink/Kafka Cloudera Manager',
                              'Unable to get to Cloudera Manager to monitor Flink Yarn App - Please check')

    else:
        print('YarnApplication on the cluster: ', resp.json()['applications'])
        if len(resp.json()['applications']) == 0:
            sendEmailNotification('Critical: No Yarn Application Found Event Filter',
                                  'Unable to find Flink Yarn App - Please check')
            sys.exit()
        else:
            application_ID = resp.json()['applications'][0]['applicationId']
            print('YarnApplicationID:', resp.json()['applications'][0]['applicationId'])
            return application_ID


def findFlinkURL():
    yarnAppId = getYarnAppId()


    roles_url= CM_URL + '/api/' + API_VERSION + '/clusters/' + CLUSTER_NAME + '/services/yarn/roles'
    print('Yarn Roles URL: ',roles_url)

    try:
        roles = requests.get(roles_url, auth=('monitor', 'monitor'))
        # print(roles.json())
        # print(type(roles.json()['items']))
        # print('Loop over the roles json list')

        # Identify active RMs host Id
        host_id_list = []
        for item in roles.json()['items']:
            # print(item)
            if item['type'] == 'RESOURCEMANAGER' and item['haStatus'] == 'ACTIVE':
                host_id= item['hostRef']['hostId']
                print('HostId = ', host_id)
                host_id_list.append(host_id)

        host_url = CM_URL + '/api/' + API_VERSION + '/hosts/' + host_id_list[0]

        # Get hostId
        resp3 = requests.get(host_url, auth=('monitor', 'monitor'))
        # print(resp3.json())
        # print(type(resp3.json()))

        print('Active Resource Manager: ', resp3.json()['hostname'])
        active_rm = resp3.json()['hostname']

        FLINK_URL = 'http://' + active_rm + ':8088' + '/proxy/' + yarnAppId + '/jobs/'

        if "application/json" in requests.get(FLINK_URL).headers['content-type']:
            return FLINK_URL

    except:
        print('Unable to determine active RM or Flink URL')

        event_subject = 'Flink could be down or not responding'
        event_body = 'Unable to get to /jobs endpoint to mointor Flink - Please check'
        sendEmailNotification(event_subject, event_body)


def monitorFlink():
    FLINK_URL = findFlinkURL()

    print('Flink URL: ', FLINK_URL)
    try:
        resp = requests.get(FLINK_URL)
        print(resp.json().keys())
        print('Jobs Running:', resp.json()['jobs-running'])
        print('Jobs Failed:', resp.json()['jobs-failed'])
        print('Jobs Cancelled:', resp.json()['jobs-cancelled'])

        # Send email if there are no running jobs
        if len(resp.json()['jobs-running']) < 1:
            print("Critical: Event Filter Flink Job is DOWN!")
            event_subject = 'Critical: Event Filter Flink Job is DOWN!'
            event_body = 'No running event filter job found. Please check the logs for more details.'
            if sendEmailNotification:
                sendEmailNotification(event_subject, event_body)

        # Send email if there is more than one running Job
        if len(resp.json()['jobs-running']) > 1:
            print("More than ONE Flink Job is RUNNING - Please check")
            event_subject = 'ERROR: More than One Flink Job is RUNNING!'
            event_body = 'More than One Flink Job is running. Please check.'
            if sendEmailNotification:
                sendEmailNotification(event_subject, event_body)

        # Send email if there are failed jobs
        if len(resp.json()['jobs-failed']) > 0:
            print("Filnk Job Failed")
            event_subject = 'Event Filter Job Failed!'
            event_body = 'Event Filter job failed. Please check the logs for more details.'
            if sendEmailNotification:
                sendEmailNotification(event_subject, event_body)
    except:
        event_subject = 'Flink could be down or not responding'
        event_body = 'Unable to get to /jobs endpoint to mointor Flink - Please check'
        sendEmailNotification(event_subject, event_body)


def main():
    monitorFlink()


if __name__ == "__main__":
    main()
