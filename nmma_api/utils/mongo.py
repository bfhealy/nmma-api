import traceback
from typing import Optional

import pymongo
from pymongo.errors import BulkWriteError

from nmma_api.utils.config import load_config
from nmma_api.utils.logs import make_log, time_stamp

config = load_config()

log = make_log("config")


class Mongo:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 27017,
        replica_set: Optional[str] = None,
        username: str = None,
        password: str = None,
        db: str = None,
        srv: bool = False,
        verbose=0,
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.replica_set = replica_set

        if srv is True:
            conn_string = "mongodb+srv://"
        else:
            conn_string = "mongodb://"

        if self.username is not None and self.password is not None:
            conn_string += f"{self.username}:{self.password}@"

        if srv is True:
            conn_string += f"{self.host}"
        else:
            conn_string += f"{self.host}:{self.port}"

        if db is not None:
            conn_string += f"/{db}"

        if self.replica_set is not None:
            conn_string += f"?replicaSet={self.replica_set}"

        self.client = pymongo.MongoClient(conn_string)
        self.db = self.client.get_database(db)

        self.verbose = verbose

    def insert_one(
        self, collection: str, document: dict, transaction: bool = False, **kwargs
    ):
        # note to future me: single-document operations in MongoDB are atomic
        # turn on transactions only if running a replica set
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_one(document, session=session)
            else:
                self.db[collection].insert_one(document)
        except Exception as e:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting document into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def insert_many(
        self, collection: str, documents: list, transaction: bool = False, **kwargs
    ):
        ordered = kwargs.get("ordered", False)
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_many(
                            documents, ordered=ordered, session=session
                        )
            else:
                self.db[collection].insert_many(documents, ordered=ordered)
        except BulkWriteError as bwe:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting documents into collection {collection}: {str(bwe.details)}",
                )
                traceback.print_exc()
        except Exception as e:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting documents into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def update_one(
        self,
        collection: str,
        filt: dict,
        update: dict,
        transaction: bool = False,
        **kwargs,
    ):
        upsert = kwargs.get("upsert", True)

        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].update_one(
                            filter=filt,
                            update=update,
                            upsert=upsert,
                            session=session,
                        )
            else:
                self.db[collection].update_one(
                    filter=filt, update=update, upsert=upsert
                )
        except Exception as e:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting document into collection {collection}: {str(e)}",
                )
                traceback.print_exc()


def init_db(config, verbose=False):
    """
    Initialize db if necessary: create the sole non-admin user
    """
    if config["database"].get("srv, False") is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    if (
        config["database"]["admin_username"] is not None
        and config["database"]["admin_password"] is not None
    ):
        conn_string += f"{config['database']['admin_username']}:{config['database']['admin_password']}@"

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    if config["database"]["replica_set"] is not None:
        conn_string += f"/?replicaSet={config['database']['replica_set']}"

    client = pymongo.MongoClient(conn_string)

    # to fix: on srv (like atlas) we can't do this
    if config["database"].get("srv", False) is not True:
        user_ids = []
        for _u in client.admin.system.users.find({}, {"_id": 1}):
            user_ids.append(_u["_id"])

        db_name = config["database"]["db"]
        username = config["database"]["username"]

        _mongo = client[db_name]

        if f"{db_name}.{username}" not in user_ids:
            _mongo.command(
                "createUser",
                config["database"]["username"],
                pwd=config["database"]["password"],
                roles=["readWrite"],
            )
            if verbose:
                log("Successfully initialized db")

        _mongo.client.close()
