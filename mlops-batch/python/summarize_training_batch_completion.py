from src.util.dynamodb_util import TrainStateDataModel
import json


# Todo-discuss on adding or updating the record for runID

def update_train_job_state_table(sku_mappingid, rerun_batchjob_id, batch_job_status_overall="SUBMITTED",
                                 runid=1) -> int:
    """
    :param sku_mappingid: haskey for the record to update
    :param rerun_batchjob_id: on submitting job a jobid is returned and for second time re-run try
    :param batch_job_status_overall: status changes  SUBMITTED on re-run
    :param runid: default is 1 and auto increments by 1
    :return: exit code
    """
    try:
        context = TrainStateDataModel(hash_key=sku_mappingid)
        TrainStateDataModel.update(context, actions=[
            TrainStateDataModel.rerun_awsbatchjob_id.set(rerun_batchjob_id),
            TrainStateDataModel.runid.set(runid + 1),
            TrainStateDataModel.awsbatch_job_status_overall.set(batch_job_status_overall)
        ])
        return 0
    except Exception as error:
        print("Error-".format(error))
        return 1


# def insert_train_job_state_table(batchjob_id, step_job_id, skuid, mapping_id, algo_execution_status,
#                                  algo_names, s3_sku_data_input_path, s3_output_bucket_name, batch_job_definition,
#                                  s3_training_output_path, s3_evaluation_output_path, run_batchjob_id, rerun_batchjob_id,
#                                  runid,
#                                  batch_job_status_overall, **kwargs) -> int:
#     """
#     Takes input all columns and saves data in TrainState DDB Table
#     batchjob_id:
#     batchjob_id:
#     step_job_id:
#     skuid:
#     mapping_id:
#     algo_execution_status:
#     algo_names: list of algorithms
#     s3_sku_data_input_path:
#     s3_output_bucket_name:
#     batch_job_definition:
#     s3_training_output_path:
#     s3_evaluation_output_path:
#     run_batchjob_id:
#     rerun_batchjob_id,runid:
#     batch_job_status_overall:
#     **kwargs
#     :return: exit code
#     """
#     try:
#
#         StateDataModel(batchjob_id=batchjob_id, step_job_id=step_job_id,
#                        skuid=skuid, mapping_id=mapping_id,
#                        algo_execution_status=algo_execution_status,
#                        algo_names=algo_names,
#                        s3_sku_data_input_path=s3_sku_data_input_path,
#                        s3_output_bucket_name=s3_output_bucket_name,
#                        batch_job_definition=batch_job_definition,
#                        s3_training_output_path=s3_training_output_path,
#                        run_batchjob_id=run_batchjob_id,
#                        rerun_batchjob_id=rerun_batchjob_id,
#                        runid=runid,
#                        batch_job_status_overall=batch_job_status_overall
#                        ).save()
#         # StateDataModel(querystring).save()
#
#         return 0
#     except Exception as error:
#         print("Error- {}".format(error))
#         return 1


def submit_aws_batch_job() -> tuple:
    """

    :return: return tuple with exit code , job_id
    """
    try:
        # TODO- write job submit logic here
        job_id = 213123133121
        pass
        return 0, job_id
    except Exception as e:
        print("Error- {}".format(e))
        return 1


def summary_train_state_table() -> json:
    """
    Method reads all the records for failed and completed.
    :return: Returns JSON for summary of job status
    """
    failed_job = TrainStateDataModel.scan(
        TrainStateDataModel.awsbatch_job_status_overall.contains("FAILED") & (TrainStateDataModel.runid == 2))
    completed_job = TrainStateDataModel.scan(TrainStateDataModel.awsbatch_job_status_overall.contains("SUCCESS"))

    return json.dumps(dict(completed_jobs=len(list(completed_job)), failed_jobs=len(list(failed_job))))


def submit_failed_job_train_state_table() -> int:
    """
    Method scans for failed jobs and submit to AWS Batch job queue for RE-RUN
    :return: Returns exit code otherwise exception
    """
    failed_job = TrainStateDataModel.scan(
        TrainStateDataModel.awsbatch_job_status_overall.contains("FAILED") & (TrainStateDataModel.runid == 1))

    for job in failed_job:
        # TODO- few parameters are coming from inputtable and few needs to be created
        status, job_id = submit_aws_batch_job()  ## this can return multiple values like job_id,status
        if not status:
            # insert_train_job_state_table(job.batchjob_id, job.step_job_id, job.mapping_id,
            #                              job.algo_execution_status,job.algo_names,
            #                              job.s3_sku_data_input_path, job.s3_output_bucket_name,
            #                              job.batch_job_definition,job.s3_training_output_path, job.s3_evaluation_output_path,
            #                              job.run_batchjob_id,job.rerun_batchjob_id, job.runid, job.batch_job_status_overall
            #                              )
            update_train_job_state_table(batch_jobid=job.sku_mappingid, rerun_batchjob_id=job_id, runid=job.runid)
        else:
            raise Exception("Inserting Job in TrainState DDB Table Failed")
