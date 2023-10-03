from dynamodb_util import TrainInputDataModel, TrainStateDataModel, TrainingAlgorithmStatus
from src.util import ddb_helper_functions
import json
import logging
import boto3
import os


def read_train_input_table():
    """
    :return: Returns iterator for the records from TrainInput DDB Table
    """
    skus = TrainInputDataModel.scan()
    return skus


def insert_train_job_state_table(batchjob_id, step_job_id, skuid, mapping_id, algo_execution_status,
                                 algo_names, s3_sku_data_input_path, algo_final_run_s3outputpaths, batch_job_definition,
                                 s3_training_output_path, s3_evaluation_output_path,
                                 cur_batchjob_id, first_run_awsbatchjob_cw_log_url,
                                 batch_job_status_overall, **kwargs) -> int:
    """
    Takes input all columns and saves data in TrainState DDB Table

    :param batch_triggered_num_runs:
    :param batchjob_id:
    :param batchjob_id:
    :param step_job_id:
    :param skuid:
    :param mapping_id:
    :param algo_execution_status:
    :param algo_names: list of algorithms
    :param s3_sku_data_input_path:
    :param algo_final_run_s3outputpaths:
    :param batch_job_definition:
    :param s3_training_output_path:
    :param s3_evaluation_output_path:
    :param cur_batchjob_id:
    :param rerun_batchjob_id:
    :param num_runs:
    :param batch_job_status_overall:
     **kwargs
    :return: exit code
    """
    # try:

    TrainStateDataModel(batchjob_id=batchjob_id,
                        step_job_id=step_job_id,
                        skuid=skuid,
                        mapping_id=mapping_id,
                        algo_execution_status=algo_execution_status,
                        algo_names=algo_names,
                        s3_sku_data_input_path=s3_sku_data_input_path,
                        algo_final_run_s3outputpaths=algo_final_run_s3outputpaths,
                        batch_job_definition=batch_job_definition,
                        s3_training_prefix_output_path=s3_training_output_path,
                        s3_evaluation_prefix_output_path=s3_evaluation_output_path,
                        cur_awsbatchjob_id=cur_batchjob_id,
                        first_run_awsbatchjob_cw_log_url=first_run_awsbatchjob_cw_log_url,
                        awsbatch_job_status_overall=batch_job_status_overall,
                        ).save()
    # StateDataModel(querystring).save()

    # except Exception as error:
    #     logging.log(error)
    #     print("Error- {}".format(error))
    #     return False
    return True


# def delete_table_record(ddb_model, column_object, column_value)->bool:
#     """
#
#     :param ddb_model: Table object ex: InputTable
#     :param column_object: primary key object INputtable.skuMapping
#     :param column_value: primary key value
#     :return:
#     """
#     try:
#
#         context = ddb_model.get(column_value)
#         context.delete(condition=(column_object == column_value))
#
#     except Exception as error:
#         logging.error(error)
#         return False
#     return True

def submit_aws_batch_job(boto3_client):
    """

    :returns status,job_id -tuple
    """

    # client = boto3.client('batch')
    client = boto3_client
    # ToDO - check batch_job_bame needs to be unique??? if yes then add sku|mapping
    aws_batch_job_name = "batch_job_name"
    aws_batch_job_queue = "arn:aws:batch:ap-south-1:731580992380:job-queue/mlops-batch_queue_ec2-poc"
    aws_batch_job_definition = "arn:aws:batch:ap-south-1:731580992380:job-definition/mlops-batch_job_ec2-poc:1"
    # TODO change commmand  hello world. py when integration happens
    response = client.submit_job(
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
    print("BATCH response {}".format(response))
    jobid = response["jobId"]
    logging.log(response)
    return True, jobid, aws_batch_job_definition


if __name__ == '__main__':

    sku_data_iterator = read_train_input_table()
    total_skus = sku_data_iterator.total_count
    batch_client = boto3.client('batch')
    for job in list(sku_data_iterator):
        # item = json.loads(item.to_json())
        # TODO- custom create s3 path training and evaluation
        # TODO- few parameters are coming from inputTable and few needs to be created

        if not TrainStateDataModel.exists():
            TrainStateDataModel.create_table(read_capacity_units=10, write_capacity_units=10)

        status, job_id, aws_batch_job_definition = submit_aws_batch_job(boto3_client=batch_client)
        # TODO- get log url needs to be umcommented
        first_run_awsbatchjob_cw_log_url = ddb_helper_functions.get_job_logstream(batchjob_id=job_id,
                                                                                  boto3_client=batch_client)
        print("AWS batch details ", status, job_id, total_skus)
        batch_job_status_overall = "SUBMITTED"
        runid = 0

        if status:
            s3_training_outpath = os.path.join(job.s3_output_bucket_name, "model")
            s3_evaluation_outpath = os.path.join(job.s3_output_bucket_name, "evaluation")
            # Insert record into TrainStateTable
            # algo_execution_status  = [TrainingAlgorithmStatus(algorithm_name=algo, algorithm_execution_status='INITIALIZING', runid=runid) for algo in job.algo_names ]
            # print(algo_execution_status)
            insert_train_job_state_table(batchjob_id=job_id, step_job_id=job.step_job_id, skuid=job.skuid,
                                         mapping_id=job.mapping_id,
                                         algo_execution_status=[],
                                         algo_names=job.algo_names,
                                         s3_sku_data_input_path=job.s3_sku_data_input_path,
                                         algo_final_run_s3outputpaths=[],
                                         batch_job_definition=aws_batch_job_definition,
                                         s3_training_output_path=s3_training_outpath,
                                         s3_evaluation_output_path=s3_evaluation_outpath,
                                         cur_batchjob_id=job_id,
                                         first_run_awsbatchjob_cw_log_url=first_run_awsbatchjob_cw_log_url,
                                         batch_job_status_overall=batch_job_status_overall)
            # Deleting record from TrainInputTable
            # ddb_helper_functions.delete_table_record(TrainInputDataModel, TrainInputDataModel.sku_mappingid, job.sku_mappingid)
        else:
            raise Exception("Job submission process steps failed")

    # check number of records
    # assert (TrainInputDataModel.count() == 0), "Failed to submit all jobs"
    assert (TrainStateDataModel.count() == total_skus), "Failed to submit all jobs"
    print(TrainStateDataModel.count())
