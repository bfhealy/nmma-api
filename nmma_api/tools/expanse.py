# This is where you can help the most Brian!

from nmma_api.utils.logs import make_log
from nmma_api.utils.config import load_config
from paramiko.client import SSHClient, AutoAddPolicy
from astropy.table import Table
from astropy.time import Time
import numpy as np
import os
import warnings

config = load_config()

local_nmma_dir = config['local']['nmma_dir']
local_data_dirname = config['local']['data_dirname']
expanse_nmma_dir = config['expanse']['nmma_dir']
expanse_data_dirname = config['expanse']['data_dirname']

slurm_script_name = config['local']['slurm_script_name']

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

    # TODO: Implement this function

    # get structure of analyses dict
    for data_dict in analyses:
        analysis_parameters = data_dict["inputs"].get("analysis_parameters", {})

        MODEL = analysis_parameters.get("source")
        resource_id = data_dict.get("resource_id", "")
        LABEL = f"{resource_id}_{MODEL}"
        TMIN = analysis_parameters.get("tmin")
        TMAX = analysis_parameters.get("tmax")
        DT = analysis_parameters.get("dt")

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
        rez = {"status": "failure", "message": "", "analysis": {}}

        try:
            data = Table.read(data_dict["inputs"]["photometry"], format="ascii.csv")
            redshift = Table.read(data_dict["inputs"]["redshift"], format="ascii.csv")
            z = redshift["redshift"][0]
        except Exception as e:
            rez.update(
                {
                    "status": "failure",
                    "message": f"input data is not in the expected format {e}",
                }
            )
            return rez
        
        # Set trigger time based on first detection
        TT = np.min(data[data["mag"] != np.ma.masked]["mjd"])

        # Give each source a different filename. This file will be copied to Expanse.
        filename = f'{resource_id}.dat'
        local_data_dir = os.path.join(local_nmma_dir, local_data_dirname)
        os.makedirs(local_data_dir, exist_ok=True)

        local_data_path = os.path.join(local_data_dir, filename)
        with open(local_data_path, 'w') as f:
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

        expanse_data_dir = os.path.join(expanse_nmma_dir, expanse_data_dirname)
        expanse.client.exec_command(f'mkdir {expanse_data_dir}')

        expanse_data_path = os.path.join(expanse_data_dir, filename)

        sftp = expanse.client.open_sftp()
        sftp.put(local_data_path, expanse_data_path)
        sftp.close()

        DATA = expanse_data_path

        _, stdout, stderr = expanse.client.exec_command(f'cd {expanse_nmma_dir}; sbatch --export=MODEL={MODEL},LABEL={LABEL},TT={TT},DATA={DATA},TMIN={TMIN},TMAX={TMAX},DT={DT} {slurm_script_name}')
        print(stdout.read().decode('utf-8'))
        submit_error = stderr.read().decode('utf-8')
        if submit_error != '':
            warnings.warn(f'Submission error: {submit_error}')
        
    return True


def retrieve() -> list[dict]:
    """Retrieve analyses results from expanse."""


if __name__ == "__main__":
    valid = validate_credentials()
    if not valid:
        log("Invalid credentials for expanse")
        exit(1)
    log("Valid credentials for expanse")
    expanse.close()
