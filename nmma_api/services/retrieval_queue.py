import time

from nmma_api.tools.expanse import retrieve
from nmma_api.tools.webhook import upload_analysis_results
from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo

log = make_log("queue")

config = load_config()

mongo = Mongo(**config["database"])
retrieval_wait_time = config["wait_times"]["retrieval"]


def retrieval_queue():
    """Retrieve analysis results from expanse."""
    while True:
        try:
            # get the analysis requests that have been processed
            analysis_requests = mongo.db.analysis.find(
                {"status": {"$in": ["running", "failed_upload"]}}
            )
            for analysis in analysis_requests:
                if (
                    analysis["status"] == "failed_upload"
                    and analysis.get("nb_upload_failures", 0) >= 10
                ):
                    log(
                        f"Analysis {analysis['_id']} has failed to upload 10 times. Skipping."
                    )
                    continue
                if analysis["status"] == "running":
                    results = retrieve(analysis)
                else:
                    try:
                        results = mongo.db.results.find_one(
                            {"analysis_id": analysis["_id"]}
                        )["results"]
                    except Exception:
                        results = retrieve(analysis)
                if results is not None:
                    if analysis["status"] != "failed_upload":
                        mongo.insert_one(
                            "results",
                            {"analysis_id": analysis["_id"], "results": results},
                        )
                    uploaded, error = upload_analysis_results(results, analysis)
                    if uploaded:
                        mongo.db.analysis.update_one(
                            {"_id": analysis["_id"]},
                            {"$set": {"status": "completed"}},
                        )
                    else:
                        mongo.db.analysis.update_one(
                            {"_id": analysis["_id"]},
                            {
                                "$set": {
                                    "status": "failed_upload",
                                    "nb_upload_failures": analysis.get(
                                        "nb_upload_failures", 0
                                    )
                                    + 1,
                                    "upload_error": error,
                                }
                            },
                        )
        except Exception as e:
            log(f"Failed to retrieve analysis results from expanse: {e}")

        time.sleep(retrieval_wait_time)


if __name__ == "__main__":
    retrieval_queue()
