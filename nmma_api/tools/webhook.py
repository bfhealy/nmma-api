import time

import requests

from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo

log = make_log("utils")

config = load_config()

mongo = Mongo(**config["database"])


def get_error_message(response: requests.Response):
    try:
        response_data: dict = response.json()
        message = response_data.get(
            "message", response_data.get("data", {}).get("message", None)
        )
    except Exception:
        message = None
    return message


def upload_analysis_results(results, data_dict, request_timeout=60):
    """
    Upload the results to the webhook.

    Parameters
    ----------
    results : dict
        The results to upload.
    data_dict : dict
        The analysis request.
    request_timeout : int, optional
        The timeout for the request in seconds, by default 60.

    Returns
    -------
    bool
        Whether the upload was successful.
    str
        The error message if the upload failed.
    """

    log(f"Uploading results to webhook: {data_dict['callback_url']}")
    if data_dict["callback_method"] != "POST":
        log("Callback URL is not a POST URL. Skipping.")
        return
    url = data_dict["callback_url"]
    n_retries = 0
    error = None
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
                error = get_error_message(response)
        except Exception as e:
            if e == requests.exceptions.Timeout:
                log("Callback URL timedout. Retrying.")
                n_retries += 1
                error = "Callback URL timedout."
            else:
                log(f"Callback URL returned error: {e}. Retrying.")
                n_retries += 1
                error = str(e)
        time.sleep(10)

    if n_retries == 10:
        log("Callback URL failed after 10 retries. Analysis results won't be uploaded.")
        return False, error
    else:
        return True, None
