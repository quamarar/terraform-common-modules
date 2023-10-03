import boto3
from botocore.exceptions import ClientError
import logging
import ndjson
import json


def fetch_all_records(ddb_model) -> list:
    """
    :param ddb_model:
    :return:
    """
    return ddb_model.scan()


def delete_ddb_table(ddb_model):
    """

    :param ddb_model:
    :return:
    """
    try:
        ddb_model.delete_table()

    except Exception as error:
        logging.error(error)
        print("Error- {}".format(error))
        return 1
    finally:
        # TODO- send sns was not able to cleanup
        pass
    return 0


def dump_data_to_s3(s3_ouput_bucket, s3_output_object_name, ddb_model):
    """
    Method fetches data from DDB table and dumps to S3 bucket
    :param s3_ouput_bucket:
    :param s3_output_object_name:
    :param ddb_model:
    :return:
    """
    # temp_file = "table_data.json"
    # out_file = open(temp_file, 'w')
    all_records = ddb_model.scan()
    all_data = [json.loads(record.to_json()) for record in all_records]

    upload_file(table_data=all_data, bucket_name=s3_ouput_bucket, object_name=s3_output_object_name)


def upload_file(table_data, bucket_name, object_name) -> bool:
    """
    Methods helps to upload data to s3
    :param table_data:
    :param bucket_name:
    :param object_name:
    :return:
    """
    try:
        s3 = boto3.resource('s3')
        s3object = s3.Object(bucket_name, object_name)

        s3object.put(
            Body=(bytes(ndjson.dumps(table_data).encode('UTF-8')))
        )
    except ClientError as e:
        return False
    return True


def delete_table_record(ddb_model, column_object, column_value) -> bool:
    """

    :param ddb_model: Table object ex: InputTable
    :param column_object: primary key object INputtable.skuMapping
    :param column_value: primary key value
    :return:
    """
    try:

        context = ddb_model.get(column_value)
        # context.refresh()
        context.delete(condition=(column_object == column_value))

    except Exception as error:
        logging.error(error)
        return False
    return True


def get_batch_container_image_arn(batch_definiton) -> str:
    batch_client = boto3.client('batch')
    response = batch_client.describe_job_definitions(
        jobDefinitions=[
            batch_definiton,
        ],
        maxResults=1,

    )
    if 0 == len(response):
        logging.warning("No data retrieved")
        return ''
    else:
        container_image_arn = response['jobDefinitions'][0]['containerProperties']['image']
        return container_image_arn


def submit_aws_batch_job(boto3_client, compute_factor_2x=False) -> tuple:
    """
    adding job to t
    :param boto3_client:
    :param compute_factor_2x:
    :return: return tuple with exit code , job_id
    """
    try:
        # TODO- update 2x logic

        # boto3_client = boto3.client('batch')
        boto3_client = boto3_client
        aws_batch_job_name = "batch_job_name"
        aws_batch_job_queue = "arn:aws:batch:ap-south-1:731580992380:job-queue/mlops-batch_queue_ec2-poc"
        aws_batch_job_definition = "arn:aws:batch:ap-south-1:731580992380:job-definition/mlops-batch_job_ec2-poc:1"
        if compute_factor_2x:
            # TODO- need to write logic here
            pass
        else:

            response = boto3_client.submit_job(
                jobName=aws_batch_job_name,
                jobQueue=aws_batch_job_queue,
                jobDefinition=aws_batch_job_definition,
                containerOverrides={
                    'command': [
                        "python", "./hello-world.py",
                    ],
                },
                retryStrategy={
                    'attempts': 1
                }
            )
            job_id = response["jobId"]

    except Exception as error:
        logging.error(error)
        print("Error- {}".format(error))
        return False
    return True, job_id, aws_batch_job_definition


def get_aws_job_status(batchjob_id, boto3_client):
    """

    :param batchjob_id:
    :param boto3_client:
    :return: string of status
    """

    response = boto3_client.describe_jobs(
        jobs=[
            batchjob_id
        ]
    )
    status = response["jobs"][0]["status"]

    return status


def get_job_logstream(batchjob_id, boto3_client):
    """

    :param batchjob_id:
    :param boto3_client:
    :return:
    """
    response = boto3_client.describe_jobs(
        jobs=[
            batchjob_id
        ]
    )
    log_stream_name = response["jobs"][0]['attempts'][0]['container']['logStreamName']

    return log_stream_name
