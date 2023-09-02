# This is where you can help the most Brian!

from nmma_api.utils.logs import make_log
from nmma_api.utils.config import load_config
from paramiko.client import SSHClient, AutoAddPolicy

config = load_config()

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


expanse = Expanse(**config["expanse"])


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
