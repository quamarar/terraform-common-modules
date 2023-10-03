from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, MapAttribute, VersionAttribute, UnicodeSetAttribute, NumberAttribute, \
    ListAttribute
from pynamodb.exceptions import TableDoesNotExist

"""
https://pynamodb.readthedocs.io/en/stable/quickstart.html
"""


class TrainingAlgorithmStatus(MapAttribute):
    algorithm_name = UnicodeAttribute()
    runid = NumberAttribute()
    algorithm_execution_status = UnicodeAttribute()

    def __eq__(self, other):
        return (isinstance(other, TrainingAlgorithmStatus)
                and self.algorithm_name == other.algorithm_name
                and self.algorithm_execution_status == other.algorithm_execution_status
                and self.runid == other.runid)

    def __repr__(self):
        return str(vars(self))


class TrainingAlgorithmS3OutputPath(MapAttribute):
    algorithm_name = UnicodeAttribute()

    model_s3_output_path = UnicodeAttribute()
    evaluation_s3_output_path = UnicodeAttribute()

    """model_s3_output_path = s3://bucketname/usecasename/training/year=execution_year
                                       /month=execution_month/day=execution_day/sku=skuid/mapping=mappingid/
                                       batch=batch_job_id/algo=algorithmname/model.tar.gz

       evaluation_s3_output_path = s3://bucketname/usecasename/evaluation/year=execution_year
                                           /month=execution_month/day=execution_day/sku=skuid/mapping=mappingid/
                                           batch=batch_job_id/algo=algorithmname/evaluation.json
    """

    def __eq__(self, other):
        return (isinstance(other, TrainingAlgorithmStatus)
                and self.algorithm_name == other.algorithm_name
                and self.model_s3_output_path == other.model_s3_output_path
                and self.evaluation_s3_output_path == other.evaluation_s3_output_path
                )

    def __repr__(self):
        return str(vars(self))


class TrainInputDataModel(Model):
    """
    A DynamoDB User
    """

    class Meta:
        table_name = 'TrainInputTable'
        region = 'ap-south-1'
        # host = "http://localhost:8000"

    # skuiid|mappingid mappingID is not being made the sort key, as there can be multiple mappingid

    sku_mappingid = UnicodeAttribute(hash_key=True)
    step_job_id = UnicodeAttribute()
    usecase_name = UnicodeAttribute()
    execution_year = UnicodeAttribute()
    execution_month = UnicodeAttribute()
    execution_day = UnicodeAttribute()
    skuid = UnicodeAttribute()
    mapping_id = UnicodeAttribute()

    # Algo names to train the individual dates set on
    algo_execution_status = ListAttribute(of=TrainingAlgorithmStatus, default=[])
    algo_names = UnicodeSetAttribute()

    # S3 path with complete file name - assumption there is only file per  S3 input Path
    s3_sku_data_input_path = UnicodeAttribute()
    # bucketname without s3:// prefix
    s3_output_bucket_name = UnicodeAttribute()
    batch_job_definition = UnicodeAttribute()

    batch_job_status_overall = UnicodeAttribute()
    version = VersionAttribute()  # TODO-to be discussed
    """s3_training_output_path = s3://bucketname/usecasename/training/year=execution_year
                                       /month=execution_month/day=execution_day/sku=skuid/mapping=mappingid/
                                       batch=batch_job_id/algo=algorithmname

       s3_evaluation_output_path = s3://bucketname/usecasename/evaluation/year=execution_year
                                           /month=execution_month/day=execution_day/sku=skuid/mapping=mappingid/
                                           batch=batch_job_id/algo=algorithmname"""


class TrainStateDataModel(Model):
    """
    A DynamoDB User
    """

    class Meta:
        table_name = 'TrainStateTable'
        region = 'ap-south-1'
        # host = "http://localhost:8000"

    # skuiid|mappingid  mappingID is not being made the sort key, as there can be multiple mappingid
    batchjob_id = UnicodeAttribute(hash_key=True)
    step_job_id = UnicodeAttribute()
    skuid = UnicodeAttribute()
    mapping_id = UnicodeAttribute()

    # Algo names to train the individual dates set on
    algo_execution_status = ListAttribute(of=TrainingAlgorithmStatus, default=[])
    algo_names = UnicodeSetAttribute()
    algo_final_run_s3outputpaths = ListAttribute(of=TrainingAlgorithmS3OutputPath, default=[])
    # S3 path with complete file name - assumption there is only file per  S3 input Path
    s3_sku_data_input_path = UnicodeAttribute()

    batch_job_definition = UnicodeAttribute()

    """s3_training_output_path = s3://bucketname/usecasename/training/year=execution_year
                                       /month=execution_month/day=execution_day/sku=skuid/mapping=mappingid/
                                       batch=batch_job_id/"""

    s3_training_prefix_output_path = UnicodeAttribute()

    """s3_evaluation_output_path = s3://bucketname/usecasename/evaluation/year=execution_year
                                           /month=execution_month/day=execution_day/sku=skuid/mapping=mappingid/
                                           batch=batch_job_id/"""

    s3_evaluation_prefix_output_path = UnicodeAttribute()

    cur_awsbatchjob_id = UnicodeAttribute()
    rerun_awsbatchjob_id = UnicodeAttribute(default="")
    rerun_awsbatchjob_cw_log_url = UnicodeAttribute(default="")
    first_run_awsbatchjob_cw_log_url = UnicodeAttribute()
    num_runs = NumberAttribute(default=0)
    awsbatch_job_status_overall = UnicodeAttribute()
    awsbatch_triggered_num_runs = NumberAttribute(default=0)
    version = VersionAttribute()
