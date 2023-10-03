from src.util.dynamodb_util import TrainInputDataModel, TrainingAlgorithmStatus

# names = UnicodeSetAttribute()
xgboost_algo = TrainingAlgorithmStatus(algorithm_name='XGBOOST', algorithm_execution_status='INITIALIZING', runid=0)
linearregression = TrainingAlgorithmStatus(algorithm_name='LINEARREGRESSION', algorithm_execution_status='INITIALIZING', runid=0)
names = ['XGBOOST', 'LINEARREGRESSION']
print(TrainInputDataModel.describe_table())
TrainInputDataModel.delete_table()


def insert_train_job_def_input_table(sku_mappingid, step_job_id, usecase_name,
                                     execution_year, execution_month, execution_day, skuid,
                                     mapping_id, algo_names_toexecute, algo_names,
                                     s3_sku_data_input_path, s3_output_bucket_name,
                                     batch_job_definition, batch_job_status_overall, **kwargs) -> int:
    """
    Takes input all columns and saves data in TrainInput DDB Table
    :param sku_mappingid: skuid|mapping_id( primary key)
    :param step_job_id:
    :param usecase_name:
    :param execution_year: yyyy
    :param execution_month: mm
    :param execution_day: dd
    :param skuid: unique string
    :param mapping_id: unique string
    :param algo_names_toexecute:
    :param s3_sku_data_input_path:
    :param s3_output_bucket_name:
    :param batch_job_definition: algo_names:
    :param batch_job_status_overall:
    **kwargs
    :return: exit code
    """

    try:
        TrainInputDataModel(sku_mappingid=sku_mappingid, step_job_id=step_job_id, usecase_name=usecase_name,
                            execution_year=execution_year, execution_month=execution_month, execution_day=execution_day,
                            skuid=skuid,
                            mapping_id=mapping_id, algo_names_toexecute=algo_names_toexecute, algo_names=algo_names,
                            s3_sku_data_input_path=s3_sku_data_input_path, s3_output_bucket_name=s3_output_bucket_name,
                            batch_job_definition=batch_job_definition,
                            batch_job_status_overall=batch_job_status_overall,
                            ).save()
        return 0
    except Exception as error:
        print("Error- {}".format(error))
        return 1


###############################
# TO BE DISCUSSED -Bulk write
#############################
# with InputDataModel.batch_write() as batch:
#     items = [InputDataModel('forum-{0}'.format(x), 'subject-{0}'.format(x)) for x in range(1000)]
#     for item in items:
#         batch.save(item)

try:
    TrainInputDataModel.create_table(read_capacity_units=1, write_capacity_units=1)
    print("Table TrainState Table Created")
except Exception as e:
    print("Error- {}".format(e))

####################################
# Read dictionary data and submit job TrainINput DDB Table
###################################
sku_info_dic = dict()

# TODO- create dictionary with all the data

for key, value in sku_info_dic.items():
    # TODO- pass all the requirements in function
    insert_train_job_def_input_table()
