import time
from datetime import datetime

from nmma_api.tools.expanse import retrieve, cancel_job, submit  # noqa 401
from nmma_api.tools.webhook import upload_analysis_results
from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo

log = make_log("retrieval_queue")

config = load_config()

mongo = Mongo(**config["database"])
retrieval_wait_time = config["wait_times"]["retrieval"]
max_upload_failures = config["wait_times"].get("max_upload_failures", 10)
time_limit = config["expansion"].get("time_limit", 6) * 3600  # in seconds

if time_limit > 24 * 3600:
    raise ValueError("time_limit cannot be greater than 24 hours")
if time_limit < 3600:
    raise ValueError("time_limit cannot be less than 1 hour")


def retrieval_queue():
    """Retrieve analysis results from expanse."""
    while True:
        try:
            # get the analysis requests that have been processed
            analysis_requests = mongo.db.analysis.find(
                {
                    "status": {
                        "$in": [
                            "running",  # analysis is running on expanse
                            "running_plot",  # analysis ran for too long, plot are being generated from checkpoints
                            "retry_upload",  # analysis has been retrieved but failed to upload back to the webhook
                            "failed_submission_to_upload",  # analysis failed to submit to expanse (didn't start at all)
                        ]
                    }
                }
            )
            analysis_requests = [x for x in analysis_requests]
            log(
                f"Found {len(analysis_requests)} analysis requests to retrieve/process."
            )
            for analysis in analysis_requests:
                # webhook has expired, can't upload results upstream anymore
                if (
                    datetime.strptime(analysis["invalid_after"], "%Y-%m-%d %H:%M:%S.%f")
                    < datetime.utcnow()
                ):
                    cancel_job(analysis.get("job_id", None))
                    log(
                        f"Analysis {analysis['_id']} webhook has expired. Skipping and deleting the results if they exist."
                    )
                    mongo.db.analysis.update_one(
                        {"_id": analysis["_id"]},
                        {"$set": {"status": "webhook_expired"}},
                    )

                    try:
                        mongo.db.results.delete_one({"analysis_id": analysis["_id"]})
                    except Exception:
                        pass
                    continue

                # analysis has been running for too long, cancel the job and set the status to job_expired
                # the submission queue will take of starting the plot generation job
                # and setting the status to "running_plot"
                if analysis["status"] == "running" and analysis.get(
                    "submitted_at"
                ) + time_limit < datetime.timestamp(datetime.utcnow()):
                    log(
                        f"Analysis {analysis['_id']} has been pending for too long. Cancelling the job and starting plot generation job."
                    )
                    cancel_job(analysis.get("job_id", None))
                    mongo.db.analysis.update_one(
                        {"_id": analysis["_id"]},
                        {"$set": {"status": "job_expired"}},
                    )

                # analysis failed to submit to expanse, update the status upstream
                if analysis["status"] == "failed_submission_to_upload":
                    log(
                        f"Analysis {analysis['_id']} failed to submit to expanse. Updating status upstream."
                    )
                    results = {
                        "status": "failure",
                        "message": analysis.get("error", "unknown error"),
                    }
                    upload_analysis_results(
                        results, analysis
                    )  # for a failure, we don't bother with retries
                    mongo.db.analysis.update_one(
                        {"_id": analysis["_id"]},
                        {"$set": {"status": "failed_submission"}},
                    )
                    continue

                # an edge case, but the plots have been generating for too long, we cancel the job, set it to failed
                # and upload that failure status upstream
                if analysis["status"] == "running_plot" and analysis.get(
                    "submitted_at"
                ) + time_limit < datetime.timestamp(datetime.utcnow()):
                    log(
                        f"Analysis {analysis['_id']} plot generation has been running for too long. Cancelling the job and setting it to failed."
                    )
                    cancel_job(analysis.get("job_id", None))
                    results = {
                        "status": "failure",
                        "message": "analysis ran for too long, and failed to generate plots",
                    }
                    upload_analysis_results(results, analysis)
                    mongo.db.analysis.update_one(
                        {"_id": analysis["_id"]},
                        {"$set": {"status": "failed_plot"}},
                    )
                    continue

                # analysis has failed to upload upstream 10 times, delete the results and skip
                if (
                    analysis["status"] == "retry_upload"
                    and analysis.get("nb_upload_failures", 0) >= max_upload_failures
                ):
                    log(
                        f"Analysis {analysis['_id']} has failed to upload 10 times. Skipping and deleting the results."
                    )
                    mongo.db.analysis.update_one(
                        {"_id": analysis["_id"]},
                        {"$set": {"status": "failed_upload"}},
                    )
                    mongo.db.results.delete_one({"analysis_id": analysis["_id"]})
                    continue

                # analysis or plot generation is running, try to retrieve the results if finished
                if analysis["status"] in ["running", "running_plot"]:
                    results = retrieve(analysis)
                else:
                    try:
                        results = mongo.db.results.find_one(
                            {"analysis_id": analysis["_id"]}
                        )["results"]
                    except Exception:
                        results = retrieve(analysis)

                if results is not None:
                    log(
                        f"Uploading results to webhook for analysis {analysis['_id']} ({analysis['resource_id']}, {analysis['created_at']})"
                    )

                    if analysis["status"] == "running":
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
                        # delete the results from the database
                        mongo.db.results.delete_one({"analysis_id": analysis["_id"]})
                    else:
                        mongo.db.analysis.update_one(
                            {"_id": analysis["_id"]},
                            {
                                "$set": {
                                    "status": "retry_upload",
                                    "nb_upload_failures": analysis.get(
                                        "nb_upload_failures", 0
                                    )
                                    + 1,
                                    "upload_error": error,
                                }
                            },
                        )
                else:
                    log(f"Analysis {analysis['_id']} has not completed yet. Skipping.")
        except Exception as e:
            log(f"Failed to retrieve analysis results from expanse: {e}")

        time.sleep(retrieval_wait_time)


if __name__ == "__main__":
    retrieval_queue()
