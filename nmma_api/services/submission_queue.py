import time

from nmma_api.tools.expanse import submit
from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo

log = make_log("queue")

config = load_config()

mongo = Mongo(**config["database"])


def submission_queue():
    """Submit analysis requests to expanse."""
    while True:
        try:
            # get the analysis requests that haven't been processed yet
            analysis_requests = mongo.db.analysis.find({"status": "pending"})
            submit(analysis_requests)
            for analysis_request in analysis_requests:
                mongo.db.analysis.update_one(
                    {"_id": analysis_request["_id"]},
                    {"$set": {"status": "running"}},
                )
        except Exception as e:
            log(f"Failed to submit analysis requests to expanse: {e}")

        time.sleep(10)


if __name__ == "__main__":
    submission_queue()
