from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from itertools import zip_longest


class SheetReaderService:
    def __init__(self, key_file_location):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=self.scopes
        )

        # Build the service object.
        self.service = build("sheets", "v4", credentials=credentials)

    def read(self, sheet_id, range, tabname, has_headers):
        range = f"{tabname}!{range}" if tabname else range
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=range).execute()
        values = result.get("values", [])

        if not values:
            print("No data found.")
        if has_headers and len(values) > 0:
            headers = values.pop(0)
            header_len = len(headers)
            values = [
                dict(list(zip_longest(headers, row))[:header_len]) for row in values
            ]
        return values
