from service.pipeline_service import PipelineService
from utilities import application_utils as utils

spark_session = utils.ApplicationUtilities().create_spark_session()

if __name__ == '__main__':
    PipelineService().execute_pipeline()

