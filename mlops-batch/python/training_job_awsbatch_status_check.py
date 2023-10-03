import logging

from src.util.dynamodb_util import TrainStateDataModel
import json
import boto3
from botocore.exceptions import ClientError
from src.util import ddb_helper_functions


# Todo-discuss on adding or updating the record for runID

def reupdate_train_job_state_table_onjobsubmission(batchjob_id, cur_batchjob_id, rerun_batchjob_id,
                                                   batch_job_status_overall, num_runs) -> int:
    """
    :param cur_batchjob_id: 
    :param batchjob_id: haskey for the record to update
    :param rerun_batchjob_id: on submitting job a jobid is returned and for second time re-run try
    :param batch_job_status_overall: status changes  SUBMITTED on re-run
    :param num_runs: default is 1 and auto increments by 1
    :return: exit code
    """
    # TODO retry. May failed on simulataneous writing
    # refresh
    try:
        context = TrainStateDataModel(hash_key=batchjob_id)
        context.refresh()
        TrainStateDataModel.update(context, actions=[
            TrainStateDataModel.cur_awsbatchjob_id.set(cur_batchjob_id),
            TrainStateDataModel.rerun_awsbatchjob_id.set(rerun_batchjob_id),
            TrainStateDataModel.num_runs.set(num_runs + 1),
            TrainStateDataModel.awsbatch_job_status_overall.set(batch_job_status_overall)
        ])
        return 0
    except Exception as error:
        print("Error-".format(error))
        return 1


def status_update_train_job_state_table(batchjob_id, batch_job_status_overall) -> int:
    """
    :param batchjob_id: haskey for the record to update
    :param batch_job_status_overall
    :return: exit code
    """
    # TODO retry. May failed on simultaneous writing
    # refresh
    try:
        context = TrainStateDataModel(hash_key=batchjob_id)
        context.refresh()
        TrainStateDataModel.update(context, actions=[

            TrainStateDataModel.awsbatch_job_status_overall.set(batch_job_status_overall)
        ])

    except Exception as error:
        print("Error-".format(error))
        return False
    return True

# 
# def dump_data_to_s3(s3_ouput_bucket, s3_output_object_name, ddb_model):
#     """
#     Method fetches data from DDB table and dumps to S3 bucket
#     :param s3_ouput_bucket:
#     :param s3_output_object_name:
#     :param ddb_model:
#     :return:
#     """
#     temp_file = "table_data.json"
#     out_file = open(temp_file, 'w')
#     all_records = ddb_model.scan()
#     for record in all_records:
#         out_file.write(record.to_json())
#     upload_file(file_object=out_file, bucket_name=s3_ouput_bucket, object_name=s3_output_object_name)
#     out_file.close()
# 
# 
# def upload_file(file_object, bucket_name, object_name):
#     """
#     Methods helps to upload data to s3
#     :param file_object:
#     :param bucket_name:
#     :param object_name:
#     :return:
#     """
#     try:
#         s3 = boto3.client('s3')
#         s3.upload_fileobj(file_object, bucket_name, object_name)
#     except ClientError as e:
#         return 1
#     return 0


def submit_failed_job_train_state_table(job, batch_client) -> int:
    """
    Method submit to AWS Batch job queue for RE-RUN and update TrainState DDB Table
    @:param job:
    @:param batch_client:
    :return: Returns exit code otherwise exception
    """
    # TODO- compute_factor_2x needs to be set True once logic is in place

    status, batch_job_id, _ = ddb_helper_functions.submit_aws_batch_job(batch_client, compute_factor_2x=False)

    reupdate_train_job_state_table_onjobsubmission(batchjob_id=job.batchjob_id, cur_batchjob_id=batch_job_id,
                                                   rerun_batchjob_id=batch_job_id,batch_job_status_overall="SUBMITTED", num_runs=job.num_runs)


def update_batch_job_status() -> int:
    all_jobs = TrainStateDataModel.scan()
    # failed_job = StateDataModel.scan(StateDataModel.batch_job_status_overall.contains("FAILED"))
    batch_client = boto3.client('batch')
    for job in all_jobs:
        if job.awsbatch_job_status_overall == "SUCCEEDED":
            pass
        cur_batch_job_status = ddb_helper_functions.get_aws_job_status(batchjob_id=job.cur_awsbatchjob_id,
                                                                       boto3_client=batch_client)
        if (job.num_runs == 0) and (cur_batch_job_status == "FAILED"):
            submit_failed_job_train_state_table(job=job, batch_client=batch_client)
        else:
            status_update_train_job_state_table(batchjob_id=job.batchjob_id,
                                                batch_job_status_overall=cur_batch_job_status)


def check_overall_batch_completion() -> bool:
    completed = True
    all_jobs = TrainStateDataModel.scan()
    # scan ->
    for job in all_jobs:

        if (job.num_runs == 0) and (job.awsbatch_job_status_overall == "FAILED"):
            return False
        if (job.awsbatch_job_status_overall != "FAILED") and (job.awsbatch_job_status_overall != "SUCCEEDED"):
            return False

    return completed


if __name__ == "__main__":
    update_batch_job_status()
    s3_bucket = "mvp-dev-apsouth1-preprocesing-bucket"
    object_name = "mvp/training_state_table_metadata/training_logs.ndjson"
    if check_overall_batch_completion():
        # TODO- update batch job cloud log streams in StateTable
        ddb_helper_functions.dump_data_to_s3(s3_ouput_bucket=s3_bucket,
                                             s3_output_object_name=object_name, ddb_model=TrainStateDataModel)
