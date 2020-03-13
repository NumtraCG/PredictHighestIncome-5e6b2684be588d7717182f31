import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b2684be588d7717182f32','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictHighestIncome_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e6b2684be588d7717182f32", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi0ef076722999cf4cd8859e9aafdb7b76', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e6b2684be588d7717182f32','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b2684be588d7717182f32','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b2684be588d7717182f33','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictHighestIncome_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e6b2684be588d7717182f32"],{"5e6b2684be588d7717182f32": PredictHighestIncome_DBFS}, "5e6b2684be588d7717182f33", spark,json.dumps( {"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "398", "mean": "", "stddev": "", "min": "AGRICULTURAL", "max": "Writers and authors", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "398", "mean": "351.57", "stddev": "3088.56", "min": "0", "max": "60746", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "398", "mean": "992.28", "stddev": "379.14", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "398", "mean": "299.09", "stddev": "2501.07", "min": "0", "max": "48334", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "398", "mean": "782.23", "stddev": "278.76", "min": "1004", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "398", "mean": "650.71", "stddev": "5555.63", "min": "0", "max": "109080", "missing": "0"}}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "398", "mean": "897.63", "stddev": "338.94", "min": "1000", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Occupation_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "398", "mean": "198.5", "stddev": "115.04", "min": "0.0", "max": "397.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "398", "mean": "33.75", "stddev": "50.29", "min": "0.0", "max": "163.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "398", "mean": "24.47", "stddev": "40.98", "min": "0.0", "max": "139.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "type": "real", "selected": "True", "stats": {"count": "398", "mean": "56.56", "stddev": "68.12", "min": "0.0", "max": "210.0", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e6b2684be588d7717182f33','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b2684be588d7717182f33','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e6b2684be588d7717182f34','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
	PredictHighestIncome_AutoML = tpot_execution.Tpot_execution.run(["5e6b2684be588d7717182f33"],{"5e6b2684be588d7717182f33": PredictHighestIncome_AutoFE}, "5e6b2684be588d7717182f34", spark,json.dumps( {"model_type": "regression", "label": "All_weekly", "features": ["Occupation", "M_workers", "M_weekly", "F_workers", "F_weekly", "All_workers"], "percentage": "50", "executionTime": "5", "sampling": "0", "sampling_value": "", "run_id": "", "model_id": "5e6b3b83be588d7717183081", "ProjectName": "ML Sample Problems", "PipelineName": "PredictHighestIncome", "pipelineId": "5e6b2684be588d7717182f31", "userid": "5df78f4be2f2eff24740bbd7", "runid": "", "url_ResultView": "http://13.68.212.36:3200", "experiment_id": "480623611921769"}))

	PipelineNotification.PipelineNotification().completed_notification('5e6b2684be588d7717182f34','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e6b2684be588d7717182f34','5df78f4be2f2eff24740bbd7','http://13.68.212.36:3200/pipeline/notify','http://13.68.212.36:3200/logs/getProductLogs')
	sys.exit(1)

