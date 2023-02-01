import boto3
import re
import json
import logging as log

def lambda_handler(event, context):

    ''' For an SES-trigger, the event follows the structure given here
    https://docs.aws.amazon.com/lambda/latest/dg/services-ses.html
    '''
    # Email parts 
    from_ = event['Records'][0]['ses']['mail']['commonHeaders']['from']
    subject = event['Records'][0]['ses']['mail']['commonHeaders']['subject']
    timestamp = event['Records'][0]['ses']['mail']['timestamp']

    # Oracle BI emails
    match = re.search(r".*Load.*(?P<completion_status>Started|completed)|Oracle.*\[(?P<file_name>\w+).*(?P<event_type>Incremental|Full|Sync Start|Sync End).*(?P<schedule_type>BIP Sch|API Req)(\)|\s)(?P<chunk_type>\d+of\d+)?.*(?P<status>successfully|failed)", subject) 

    # Code to extract info taken from Andre Gonclaves' original operator
    if match:

        # Breakdown email subject
        # Capturing Load Plan Start and Completed
        completion_status = match.group('completion_status')
        log.info(f'Completion status is {completion_status}')
        # Avoiding None when email is regarding a single file or sync start / end
        chunk_type_ = '-' if match.group('chunk_type') == None else match.group('chunk_type').replace('of','/').lower()
        log.info(f'Chunk type is {chunk_type_}')
        
        # If email is not from Oracle Plan Started or Completed 
        if completion_status == None:
            file_name = match.group('file_name').lower()
            event_type = match.group('event_type').replace('\r\n','').lower()
            schedule_type = match.group('schedule_type').lower()
            chunk_type = 'single file' if '/' not in chunk_type_ else chunk_type_
            status = match.group('status').lower()

            # Dictionary stores only unique values
            # Chunked files contains '/'. Example: 'w_inventory_daily_bal_f 1/4' 
            # Merging file_name + chunk_type
            if '/' in chunk_type:
                file_name = f'{file_name} {chunk_type}'
        # Capturing Load Plan Start and Completed
        else: 
            file_name = f'load plan {completion_status}'
            schedule_type = 'oracle erp to bi' 
            chunk_type = ''
            event_type = completion_status
            status = ''

        # Build the key-value to store in dynamodb
        key = file_name
        val = {'timestamp': timestamp,
            'schedule_type': schedule_type, 
            'chunk_type': chunk_type,
            'event_type': event_type,
            'status': status}
        
        # Initiate boto3 dynamodb session
        ddb_client = boto3.resource('dynamodb',
                    region_name = 'us-west-2')
        table = ddb_client.Table('email_notification_states')

        # Writing to dynamodb
        print({'file_name':key,'value':val})
        resp = table.put_item(Item = {'file_name':key,'value':json.dumps(val)})
        
        # Deal with response 
        if not resp['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise Exception(f'Error updating dynamodb: {resp}')
        else:
            print(f'Dynamodb updated successfully: {resp}')