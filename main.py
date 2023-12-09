from service.pipeline_service import PipelineService
from quality.data_quality import DataQuality
from utilities import application_utils as utils
from analysis import data_analysis

spark_session = utils.ApplicationUtilities().create_spark_session()

if __name__ == '__main__':
    PipelineService().execute_pipeline()
    data_analysis.Analysis().perform_analysis()

