# delete input table

# loop through state table
# and terminate all the jobs
# delete state table
import logging
import boto3
from src.util.dynamodb_util import TrainStateDataModel, TrainInputDataModel
from src.util import ddb_helper_functions


def clean_up_framework():
    input_records = ddb_helper_functions.delete_ddb_table(TrainInputDataModel)
    all_jobs = ddb_helper_functions.fetch_all_records(TrainStateDataModel)
    batch_client = boto3.client('batch')
    for job in all_jobs:
        try:
            logging.log("Terminating AWS Btach JOBID- {}".format(job.cur_awsbatchjob_id))
            response = batch_client.terminate_job(
                jobId=job.cur_awsbatchjob_id,
                reason='cleanup'
            )
        except Exception as error:
            logging.error(job.cur_awsbatchjob_id, error)
    ddb_helper_functions.delete_ddb_table(TrainStateDataModel)
    state_records = ddb_helper_functions.fetch_all_records(TrainStateDataModel)

    assert (0 == len(list(input_records))), "Error in deleting records from StateTable"
    assert (0 == len(list(state_records))), "Error in deleting records from InputTable"
    return True


if __name__ == '__main__':
    clean_up_framework()
