import logging
import sqlalchemy as db
from sqlalchemy import update, bindparam
from sqlalchemy.orm import Session

class Database:
    def __init__(self, connection_url):
        self.engine = db.create_engine(connection_url)
        self.conn = self.engine.connect()
        self.metadata = db.MetaData()

    def _verify_columns(self, rows, table_columns, db_config):
        """
        Extra columns in sheets should raise exception
        Extra columns in tables should be ok
        """
        exact_match = db_config.get("exact_match", True)

        
        cols_in_sheet = [col.lower() for col in rows[0].keys()]
        cols_in_table = [c.name.lower() for c in table_columns]
        extra_cols = set(cols_in_table) - set(cols_in_sheet)
        logging.debug(f"table has {len(table_columns)} columns")
        logging.debug(f"data has {len(rows[0].keys())}")
        if extra_cols:
            if exact_match:
                raise Exception(
                    f"incoming data have {len(extra_cols)} unmatched columns {list(extra_cols)}"
                )

    def _check_pk_in_data(self, rows, pk):
        return all(pk in row.keys() for row in rows)

    def _get_table(self, config):
        table = config["table"]
        return db.Table(table, self.metadata, autoload_with=self.engine)

    def _get_pk(self, table, config):
        pk = config.get("pk")
        if pk:
            pk = pk.strip()
            if pk not in table.columns:
                raise ValueError(f"Invalid primary key {pk} for {table.name}")
        return pk

    def write(self, rows, config):
        """
            rows is list of dict, each dict will have keys corresponding to a column in table
        """
        if not rows:
            logging.info(f"no rows received")
            return

        logging.info(f"config: {config}")
        table = self._get_table(config)

        pk = self._get_pk(table, config)
        if pk:
            self._check_pk_in_data(rows, pk)

        self._verify_columns(rows, table.columns, config)

        if not pk:
            self._delete_and_insert(rows, table)
        else:
            self._update_using_pk(pk, rows, table)

    def _update(self, rows, table, pk, session):
        if not rows:
            return
        keys_in_row = rows[0].keys()
        update_stmt = (
            update(table).
            where(getattr(table.c, pk) == bindparam(pk)).
            values(**{k:bindparam(k) for k in keys_in_row})
            )
        logging.info(f"updating {len(rows)} rows")
        session.execute(update_stmt, rows)

    def _delete(self, ids, table, pk, session):
        if ids:
            logging.info(f"deleting {len(ids)} rows")
            logging.debug(f"deleting {ids}")
            session.execute(table.delete().where(getattr(table.c, pk).in_(ids)))

    def _insert(self, rows, table, session):
        if rows:
            logging.info(f"inserting {len(rows)} rows")
            session.execute(table.insert(), rows)

    def _categorize(self, rows, table, pk):
        existing_pks = Session(self.engine).query(getattr(table.c, pk)).all()
        existing_pks = set(entry[0] for entry in existing_pks if entry[0])
        logging.debug(f"existing IDs: {existing_pks}")
        logging.info(f"existing IDs: {len(existing_pks)}")
        ids_in_rows = set()
        insert = []
        update = []
        for row in rows:
            row_pk = row[pk]
            ids_in_rows.add(row_pk)
            if row_pk not in existing_pks:
                insert.append(row)
            else:
                update.append(row)
        logging.debug(f"sheet ids: {ids_in_rows}")
        logging.info(f"sheet ids: {len(ids_in_rows)}")
        delete = list(existing_pks - ids_in_rows)
        return delete, update, insert

    def _update_using_pk(self, pk, rows, table):
        ids_to_delete, rows_to_update, rows_to_insert = self._categorize(rows, table, pk)
        session = Session(self.engine)
        with session.begin():
            # split the rows into insert and updates
            # get all IDs from database
            self._delete(ids_to_delete, table, pk, session)
            self._update(rows_to_update, table, pk, session)
            self._insert(rows_to_insert, table, session)

    def _delete_and_insert(self, rows, table):
        session = Session(self.engine)
        with session.begin():
            logging.info("deleting all rows")
            session.execute(table.delete())
            logging.info("inserting all rows")
            session.execute(table.insert(), rows)
