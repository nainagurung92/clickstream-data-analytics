from service.pipeline_service import PipelineService
from quality.data_quality import DataQuality
from utilities import application_utils as utils
from analysis import data_analysis

spark_session = utils.ApplicationUtilities().create_spark_session()

if __name__ == '__main__':
    PipelineService().execute_pipeline()
    check_result: bool = DataQuality().executeDataQualityChecks()
    if check_result:
        print("\nAll Data Quality Passed.")
    else:
        print("\nData Quality Failed.")
    data_analysis.Analysis().perform_analysis()

