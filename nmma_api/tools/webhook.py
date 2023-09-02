import time

import requests

from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo

log = make_log("utils")

config = load_config()

mongo = Mongo(**config["database"])


def upload_analysis_results(results, data_dict, request_timeout=60):
    """
    Upload the results to the webhook.
    """

    log(f"Uploading results to webhook: {data_dict['callback_url']}")
    if data_dict["callback_method"] != "POST":
        log("Callback URL is not a POST URL. Skipping.")
        return
    url = data_dict["callback_url"]
    n_retries = 0
    while n_retries < 10:
        try:
            response = requests.post(
                url,
                json=results,
                timeout=request_timeout,
            )
            if response.status_code == 200:
                log("Results uploaded successfully.")
                break
            else:
                log(
                    f"Callback URL returned status code {response.status_code}. Retrying."
                )
                n_retries += 1
        except Exception as e:
            if e == requests.exceptions.Timeout:
                log("Callback URL timedout. Retrying.")
                n_retries += 1
            else:
                log(f"Callback URL returned error: {e}. Retrying.")
                n_retries += 1
        time.sleep(10)

    if n_retries == 10:
        log("Callback URL failed after 10 retries. Analysis results won't be uploaded.")

    # in any case, save the results to the database
    mongo.insert_one("results", results)
