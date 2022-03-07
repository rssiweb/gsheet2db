import sys
import os
import json
import logging
from google_sheet import SheetReaderService
from target.database import Database
import pathlib


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d: %(message)s",
    level=logging.DEBUG,
    stream=sys.stdout,
)
logging.getLogger()

base_path = pathlib.Path(__file__).parent.resolve()
def read_service_creds(key_file_location):
    if not os.path.exists(key_file_location):
        data = os.getenv("CREDENTIALS_SERVICE_ACCOUNT")
        with open(key_file_location, "w") as f:
            f.write(data)


def get_configurations():
    with open(os.path.join(base_path, "config.json"), "r") as config:
        return json.loads(config.read())


def read_data(service, sheet_config):
    sheet_id = sheet_config["id"]
    range = sheet_config["range"]
    tabname = sheet_config.get("name")
    has_header = sheet_config.get("has_header", True)
    return service.read(sheet_id, range, tabname, has_header)


def run():
    configs = get_configurations()
    logging.info(f"{len(configs)} configs found")

    key_file_location = "service.credentials.json"
    read_service_creds(key_file_location)

    database_url = os.environ["DATABASE_URL"]

    service = SheetReaderService(key_file_location)
    db = Database(database_url)
    result = {}
    for config in configs:
        name = config["name"]
        try:
            read_config = config["sheet"]
            write_config = config["database"]
        except KeyError:
            message = f"Invalid config {config}"
            logging.exception(message)
            result[name] = message
        try:
            data = read_data(service, read_config)
            logging.info(f"read success {len(data)} rows read")
        except Exception:
            message = f"Reading sheet failed: {read_config}"
            logging.exception(message)
            result[name] = message
        else:
            try:
                db.write(data, write_config)
                logging.info("write success")
            except Exception:
                message = f"failed writing to database {write_config}"
                logging.exception(message)
                result[name] = message
            else:
                result[name] = "Loaded successfully"


if __name__ == "__main__":
    run()
