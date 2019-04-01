import json
import time

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = './ga_to_s3/client-secrets.json'
VIEW_ID = '114575665'


def init_ga_api():
    creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_LOCATION, SCOPES)
    ga_api = build('analyticsreporting', 'v4', credentials=creds)
    return ga_api


def build_ga_body(view_id=VIEW_ID, start_date='2019-01-01', end_date='today', metrics=[], dimensions=[], page_token=0):
    return {
        'reportRequests': [
            {
                'viewId': view_id,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'metrics': [{'expression': f'ga:{metric}'} for metric in metrics],
                'dimensions': [{'name': f'ga:{dimension}'} for dimension in dimensions],
                'pageToken': str(page_token),
            }
        ]
    }


def exe_ga_body(ga_api, body):
    return ga_api.reports().batchGet(body=body).execute()


def get_n_sessions(ga_api, page_token=0):
    print(page_token)
    body = build_ga_body(metrics=['sessions'], dimensions=['date'], start_date='2019-03-22', page_token=page_token)
    result = exe_ga_body(ga_api, body)
    report = result['reports'][0]
    if 'nextPageToken' in report:
        next_res = get_n_sessions(ga_api, page_token=report.get('nextPageToken'))
        try:
            next_report = next_res['reports'][0]
            report['data']['rows'].extend(next_report['data']['rows'])
            del report['nextPageToken']
        except Exception as e:
            print(f"oh no exception: {str(e)}; {type(e)}")
            print(str(next_res)[:1000])
            raise e
    return result


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        {
            "resource": "Resource path",
            "path": "Path parameter",
            "httpMethod": "Incoming request's method name"
            "headers": {Incoming request headers}
            "queryStringParameters": {query string parameters }
            "pathParameters":  {path parameters}
            "stageVariables": {Applicable stage variables}
            "requestContext": {Request context, including authorizer-returned key-value pairs}
            "body": "A JSON string of the request payload."
            "isBase64Encoded": "A boolean flag to indicate if the applicable request payload is Base64-encode"
        }

        https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

    Attributes
    ----------

    context.aws_request_id: str
         Lambda request ID
    context.client_context: object
         Additional context when invoked through AWS Mobile SDK
    context.function_name: str
         Lambda function name
    context.function_version: str
         Function version identifier
    context.get_remaining_time_in_millis: function
         Time in milliseconds before function times out
    context.identity:
         Cognito identity provider context when invoked through AWS Mobile SDK
    context.invoked_function_arn: str
         Function ARN
    context.log_group_name: str
         Cloudwatch Log group name
    context.log_stream_name: str
         Cloudwatch Log stream name
    context.memory_limit_in_mb: int
        Function memory

        https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict
        'statusCode' and 'body' are required

        {
            "isBase64Encoded": true | false,
            "statusCode": httpStatusCode,
            "headers": {"headerName": "headerValue", ...},
            "body": "..."
        }

        # api-gateway-simple-proxy-for-lambda-output-format
        https: // docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)
    #
    #     raise e

    ga_api = init_ga_api()
    res = json.dumps(get_n_sessions(ga_api))

    with open(f'nsw-election-to-now-sessions-by-day-{time.time()}.json', 'w+') as f:
        f.write(res)

    return {
        "statusCode": 200,
        "body": res,
    }
