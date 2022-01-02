import logging
import sqlalchemy as db
from sqlalchemy.orm import Session


class Database:
    def __init__(self, connection_url):
        self.engine = db.create_engine(connection_url)
        self.conn = self.engine.connect()
        self.metadata = db.MetaData()

    def _verify_columns(self, rows, columns, db_config):
        exact_match = db_config.get("exact_match", True)

        if rows:
            database_cols = [col.lower() for col in rows[0].keys()]
            unmatched_columns = set(database_cols) - set(
                [c.name.lower() for c in columns]
            )
            logging.debug(f"database table has {len(columns)} columns")
            logging.debug(f"data has {len(rows[0].keys())}")
            if unmatched_columns:
                if exact_match:
                    raise Exception(
                        f"incoming data have {len(unmatched_columns)} unmatched columns {list(unmatched_columns)}"
                    )
        return rows

    def write(self, rows, db_config):
        logging.info(f"config: {db_config}")
        table = db_config["table"]
        mode = db_config.get("mode", "overwrite")
        if not rows:
            return
        # convert values without columns to columns with column names
        if not isinstance(rows[0], dict):
            header = db_config["columns"]
            rows = [zip(header, row) for row in rows]
        logging.info(f"{len(rows)} will be inserted")
        table = db.Table(table, self.metadata, autoload_with=self.engine)
        self._verify_columns(rows, table.columns, db_config)

        session = Session(self.engine)
        with session.begin():

            if mode == "overwrite":
                logging.info("deleting all rows")
                session.execute(table.delete())
            logging.info("inserting all rows")
            session.execute(table.insert(), rows)
