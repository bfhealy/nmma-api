import base64
import gzip
import json
import os
import tempfile
import warnings

import arviz as az
import joblib
import numpy as np
from astropy.table import Table
from astropy.time import Time
from paramiko.client import SSHClient, AutoAddPolicy

from nmma_api.utils.logs import make_log
from nmma_api.utils.config import load_config


config = load_config()

local_nmma_dir = config["local"]["nmma_dir"]
local_data_dirname = config["local"]["data_dirname"]
local_output_dirname = config["local"]["output_dirname"]

expanse_nmma_dir = config["expanse"]["nmma_dir"]
expanse_data_dirname = config["expanse"]["data_dirname"]
expanse_output_dirname = config["expanse"]["output_dirname"]

local_data_dir = os.path.join(local_nmma_dir, local_data_dirname)
local_output_dir = os.path.join(local_nmma_dir, local_output_dirname)

expanse_data_dir = os.path.join(expanse_nmma_dir, expanse_data_dirname)
expanse_output_dir = os.path.join(expanse_nmma_dir, expanse_output_dirname)

slurm_script_name = config["local"]["slurm_script_name"]

log = make_log("expanse")


class Expanse:
    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = SSHClient()
        self.client.set_missing_host_key_policy(AutoAddPolicy())
        self.client.connect(
            self.host, port=self.port, username=self.username, password=self.password
        )

    def reconnect(self):
        self.client.connect(self.host, username=self.username, password=self.password)

    def close(self):
        self.client.close()


expanse = Expanse(**config["expanse"]["ssh"])


def validate_credentials() -> bool:
    """Validate the credentials for expanse."""
    try:
        stdin, stdout, stderr = expanse.client.exec_command("echo 'hello world'")
        if stdout.read() != b"hello world\n":
            return False
        return True
    except Exception as e:
        log(f"Failed to validate credentials for expanse: {e}")
        return False


def submit(analyses: list[dict], **kwargs) -> bool:
    """Submit an analysis to expanse."""
    jobs = {}
    log(f"Submitting {len(analyses)} analysis requests to expanse")

    for data_dict in analyses:
        try:
            try:
                analysis_parameters = data_dict["inputs"].get("analysis_parameters", {})
                timestamp = data_dict.get("created_at", None)
                status = data_dict.get("status", None)

                MODEL = analysis_parameters.get("source")
                resource_id = data_dict.get("resource_id", "")
                LABEL = f"{resource_id}_{timestamp}"
                TMIN = analysis_parameters.get("tmin")
                TMAX = analysis_parameters.get("tmax")
                DT = analysis_parameters.get("dt")
                SKIP_SAMPLING = ""
                if status == "expired":
                    SKIP_SAMPLING = "--skip-sampling"

                # this example analysis service expects the photometry to be in
                # a csv file (at data_dict["inputs"]["photometry"]) with the following columns
                # - filter: the name of the bandpass
                # - mjd: the modified Julian date of the observation
                # - magsys: the mag system (e.g. ab) of the observations
                # - flux: the flux of the observation
                #
                # the following code transforms these inputs from SkyPortal
                # to the format expected by nmma.
                #

                # first, decompress the data
                data_decompressed = gzip.decompress(
                    data_dict["inputs"]["photometry"]
                ).decode()
                redshift_decompressed = gzip.decompress(
                    data_dict["inputs"]["redshift"]
                ).decode()
                data = Table.read(data_decompressed, format="ascii.csv")
                redshift = Table.read(redshift_decompressed, format="ascii.csv")
                z = redshift["redshift"][0]  # noqa F841
            except Exception as e:
                raise ValueError(f"input data is not in the expected format {e}")

            try:
                # Set trigger time based on first detection
                TT = np.min(data[data["mag"] != np.ma.masked]["mjd"])

                # Give each source a different filename. This file will be copied to Expanse.
                filename = f"{resource_id}_{timestamp}.dat"
                os.makedirs(local_data_dir, exist_ok=True)

                local_data_path = os.path.join(local_data_dir, filename)
                with open(local_data_path, "w") as f:
                    # output the data in the format desired by NMMA:
                    # remove rows where mag and magerr are missing, or not float, or negative
                    data = data[
                        np.isfinite(data["mag"])
                        & np.isfinite(data["magerr"])
                        & (data["mag"] > 0)
                        & (data["magerr"] > 0)
                    ]
                    for row in data:
                        tt = Time(row["mjd"], format="mjd").isot
                        filt = row["filter"]
                        mag = row["mag"]
                        magerr = row["magerr"]
                        f.write(f"{tt} {filt} {mag} {magerr}\n")
            except Exception as e:
                raise ValueError(f"failed to format data {e}")

            try:
                expanse.client.exec_command(f"mkdir {expanse_data_dir}")

                expanse_data_path = os.path.join(expanse_data_dir, filename)

                sftp = expanse.client.open_sftp()
                sftp.put(local_data_path, expanse_data_path)
                sftp.close()

                DATA = expanse_data_path

                _, stdout, stderr = expanse.client.exec_command(
                    f"cd {expanse_nmma_dir}; sbatch --export=MODEL={MODEL},LABEL={LABEL},TT={TT},DATA={DATA},TMIN={TMIN},TMAX={TMAX},DT={DT},SKIP_SAMPLING={SKIP_SAMPLING} {slurm_script_name}"
                )
            except Exception as e:
                raise ValueError(f"failed to submit job {e}")

            submit_message = stdout.read().decode("utf-8").strip()
            submit_error = stderr.read().decode("utf-8").strip()

            if submit_error != "":
                warnings.warn(f"Submission error: {submit_error}")
                raise ValueError(f"Submission error: {submit_error}")
            else:
                job_id = int(submit_message.split(" ")[-1].strip())
                jobs[data_dict["_id"]] = {"job_id": job_id, "message": ""}
                log(f"Submitted job {job_id} for analysis {data_dict['_id']}")
        except Exception as e:
            log(f"Failed to submit analysis {data_dict['_id']} to expanse: {e}")
            jobs[data_dict["_id"]] = {"job_id": None, "message": str(e)}
    return jobs


def retrieve(analysis: dict) -> dict:
    """Retrieve analyses results from expanse."""
    # retrieve the results from expanse
    # look into the expanse directory for the results
    # copy the results to the local directory
    # update the database
    # return the results

    log(
        f"Retrieving results for analysis {analysis['_id']} ({analysis['resource_id']}, {analysis['created_at']})"
    )

    LABEL = f"{analysis['resource_id']}_{analysis['created_at']}"
    os.makedirs(os.path.join(local_output_dir, LABEL), exist_ok=True)

    posterior_file = os.path.join(
        expanse_output_dir, f"{LABEL}/{LABEL}_posterior_samples.dat"
    )
    local_posterior_file = os.path.join(
        local_output_dir, f"{LABEL}/{LABEL}_posterior_samples.dat"
    )

    json_file = os.path.join(expanse_output_dir, f"{LABEL}/{LABEL}_result.json")
    local_json_file = os.path.join(local_output_dir, f"{LABEL}/{LABEL}_result.json")

    lightcurves_file = os.path.join(
        expanse_output_dir, f"{LABEL}/{LABEL}_lightcurves.png"
    )
    local_lightcurves_file = os.path.join(
        local_output_dir, f"{LABEL}/{LABEL}_lightcurves.png"
    )

    local_temp_files = []

    sftp = expanse.client.open_sftp()

    try:
        # Check if results files exist
        sftp.stat(posterior_file)
        sftp.stat(json_file)
        sftp.stat(lightcurves_file)

        # Download files to local directory
        sftp.get(posterior_file, local_posterior_file)
        sftp.get(json_file, local_json_file)
        sftp.get(lightcurves_file, local_lightcurves_file)

        # Structure files to prepare for return
        tab = Table.read(local_posterior_file, format="csv", delimiter=" ")
        inference = az.convert_to_inference_data(tab.to_pandas().to_dict(orient="list"))

        f = tempfile.NamedTemporaryFile(
            suffix=".nc", prefix="inferencedata_", delete=False
        )
        f.close()

        inference.to_netcdf(f.name)
        inference_data = base64.b64encode(open(f.name, "rb").read()).decode()
        local_temp_files.append(f.name)

        with open(local_json_file) as f:
            result = json.load(f)
        log_bayes_factor = result["log_bayes_factor"]
        f = tempfile.NamedTemporaryFile(suffix=".png", prefix="nmmaplot_", delete=False)
        f.close()

        plot_data = base64.b64encode(open(local_lightcurves_file, "rb").read()).decode()
        local_temp_files.append(f.name)

        f = tempfile.NamedTemporaryFile(
            suffix=".joblib", prefix="results_", delete=False
        )
        f.close()

        joblib.dump(result, f.name, compress=3)
        result_data = base64.b64encode(open(f.name, "rb").read()).decode()
        local_temp_files.append(f.name)

        analysis_results = {
            "inference_data": {"format": "netcdf4", "data": inference_data},
            "plots": [{"format": "png", "data": plot_data}],
            "results": {"format": "joblib", "data": result_data},
        }

        results = {
            "analysis": analysis_results,
            "status": "success",
            "message": f"Good results with log Bayes factor={log_bayes_factor}",
        }

    except FileNotFoundError:
        return None
    finally:
        sftp.close()
        for f in local_temp_files:
            try:
                os.remove(f)
            except:  # noqa E722
                pass

    return results


def cancel_job(job_id: int) -> bool:
    """Cancel a job on expanse."""
    if job_id is None:
        return False
    try:
        _, stdout, stderr = expanse.client.exec_command(f"scancel {job_id}")
        # TODO: verify that the cancel error is in that format
        cancel_error = stderr.read().decode("utf-8").strip()

        if cancel_error != "":
            warnings.warn(f"Cancel error: {cancel_error}")
            raise ValueError(f"Cancel error: {cancel_error}")
        else:
            log(f"Cancelled job {job_id}")
    except Exception as e:
        log(f"Failed to cancel job {job_id} on expanse: {e}")
        return False
    return True


if __name__ == "__main__":
    valid = validate_credentials()
    if not valid:
        log("Invalid credentials for expanse")
        exit(1)
    log("Valid credentials for expanse")
    expanse.close()
