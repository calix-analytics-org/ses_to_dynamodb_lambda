import boto3
import re
import json

def lambda_handler(event, context):

    ''' For an SES-trigger, the event follows the structure given here
    https://docs.aws.amazon.com/lambda/latest/dg/services-ses.html
    '''
    from_ = event['Records'][0]['ses']['mail']['commonHeaders']['from']
    subject = event['Records'][0]['ses']['mail']['commonHeaders']['subject']
    timestamp = event['Records'][0]['ses']['mail']['timestamp']

    # Oracle BI emails
    match = re.search(r".*(\[(?P<file_name>\w+).*(?P<event_type>Incremental|Full|Sync\r\n.*Start|Sync\r\n.*End)(\r\n)?.*(\r\n.*)?(?P<chunk_type>Sch|\dof\d).*\]):\s.*(Job\r\n)?.*(?P<status>successfully|failed).*", subject)

    # Code to extract info taken from Andre Gonclaves' original operator
    if match:
        # Breakdown email subject
        file_name = match.group('file_name').lower()
        chunk_type = 'single file' if match.group('chunk_type') == 'Sch' else match.group('chunk_type').replace('of','/').lower()
        event_type = match.group('event_type').replace('\r\n','').lower()
        status = match.group('status').lower()
        
        # Dictionary only stores unique values.                   
        # Chunked files contains '/'. Example: 'w_inventory_daily_bal_f 1/4' 
        # Merging file_name + chunk_type
        if '/' in chunk_type:
            file_name = f'{file_name} {chunk_type}'
        
        # Build the key-value to store in dynamodb
        key = match.group('file_name').lower()
        val = {'timestamp': timestamp,
            'status': status}
        
        # Initiate boto3 dynamodb session
        ddb_client = boto3.resource('dynamodb',
                    region_name = 'us-west-2')
        table = ddb_client.Table('email_notification_states')

        print({'file_name':key,'value':val})
        resp = table.put_item(Item = {'file_name':key,'value':json.dumps(val)})
        if not resp['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise Exception(f'Error updating dynamodb: {resp}')
        else:
            print(f'Dynamodb updated successfully: {resp}')