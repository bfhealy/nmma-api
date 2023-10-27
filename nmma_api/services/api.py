import json
import traceback
from datetime import datetime

import tornado.escape
import tornado.web

from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log
from nmma_api.utils.mongo import Mongo, init_db
from nmma_api.tools.expanse import validate_credentials

log = make_log("main")

config = load_config()

mongo = Mongo(**config["database"])

ALLOWED_MODELS = ["Me2017", "Piro2021", "nugent-hyper", "TrPi2018", "Bu2022Ye"]
REQUEST_REQUIRED_KEYS = ["inputs", "callback_url", "callback_method"]


def validate(data: dict) -> str:
    """Validate the data_dict to make sure it has the required keys and the model is allowed."""
    missing_keys = [key for key in REQUEST_REQUIRED_KEYS if key not in data]
    if len(missing_keys) > 0:
        return f"missing required key(s) {missing_keys} in data_dict"

    model = data["inputs"].get("analysis_parameters", {}).get("source", None)
    if model is None:
        return "model not specified in data_dict.inputs.analysis_parameters"
    elif model not in ALLOWED_MODELS:
        return (
            f"model {model} is not allowed, must be one of: {ALLOWED_MODELS.join(',')}"
        )

    return None


class MainHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")

    def error(self, code, message):
        self.set_status(code)
        self.write({"message": message})

    def get(self):
        self.write({"status": "active"})

    def post(self):
        """
        Analysis endpoint which sends the `data_dict` off for
        processing, returning immediately. The idea here is that
        the analysis model may take awhile to run so we
        need async behavior.
        """

        try:
            data_dict = tornado.escape.json_decode(self.request.body)
        except json.decoder.JSONDecodeError:
            err = traceback.format_exc()
            log(f"JSON decode error: {err}")
            return self.error(400, "Invalid JSON")

        # validate
        err = validate(data_dict)
        if err is not None:
            log(f"Validation error: {err}")
            return self.error(400, err)

        # insert into database
        data = {
            **data_dict,
            "status": "pending",
            "created_at": datetime.timestamp(datetime.utcnow()),
        }
        mongo.insert_one("analysis", data)

        return self.write(
            {
                "status": "pending",
                "message": "nmma_analysis_service: analysis submitted",
            }
        )


class HealthHandler(tornado.web.RequestHandler):
    def get(self):
        # check if the database is up
        health = {
            "database": True,
            "expanse": True,
        }
        try:
            mongo.db.command("ping")
        except Exception:
            health["database"] = False

        # check if the expanse credentials are valid
        try:
            valid = validate_credentials()
            if not valid:
                health["expanse"] = False
        except Exception:
            health["expanse"] = False

        self.write(health)
        self.set_status(200)


def make_app():
    return tornado.web.Application(
        [
            (r"/analysis", MainHandler),
            (r"/health", HealthHandler),
            (r"/", HealthHandler),
        ]
    )


if __name__ == "__main__":
    init_db(config)
    app = make_app()
    port = config["ports"]["api"]
    app.listen(port)
    log(f"NMMA Service Listening on port {port}")
    tornado.ioloop.IOLoop.current().start()
