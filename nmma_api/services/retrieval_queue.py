import time

from nmma_api.tools.expanse import retrieve
from nmma_api.tools.webhook import upload_analysis_results
from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo

log = make_log("queue")

config = load_config()

mongo = Mongo(**config["database"])


def retrieval_queue():
    """Retrieve analysis results from expanse."""
    while True:
        try:
            # get the analysis requests that have been processed
            analysis_requests = mongo.db.analysis.find({"status": "running"})
            for analysis in analysis_requests:
                results = retrieve(analysis)
                if results is not None:
                    mongo.db.analysis.update_one(
                        {"_id": analysis["_id"]},
                        {"$set": {"status": "complete"}},
                    )
                    mongo.db.results.insert_one(results)
                    upload_analysis_results(results, analysis)
        except Exception as e:
            log(f"Failed to retrieve analysis results from expanse: {e}")

        time.sleep(10)


if __name__ == "__main__":
    retrieval_queue()
