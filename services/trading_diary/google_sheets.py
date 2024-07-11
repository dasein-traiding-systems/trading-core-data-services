import asyncio
import json
import os
from typing import Any, Callable, List

from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds

from config import GOOGLE_SERVICE_KEY_FILE_NAME, TRADING_DIARY_SPREEDSHEET_ID


class GoogleSheets(object):
    def __init__(self, spreadsheet_id, google_docs_creds_file_name):
        self.spreadsheetId = spreadsheet_id
        service_account_key = json.load(
            open(os.path.expanduser(google_docs_creds_file_name))
        )
        self.creds = ServiceAccountCreds(
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
            **service_account_key
        )
        self.aiogoogle = Aiogoogle(service_account_creds=self.creds)
        self.sheets = None

    async def init(self):
        self.sheets = await self.aiogoogle.discover("sheets", "v4")

    async def get_sheet_names(self):
        data = await self.aiogoogle.as_service_account(
            self.sheets.spreadsheets.get(spreadsheetId=self.spreadsheetId)
        )

        result = [s["properties"]["title"] for s in data.get("sheets", [])]

        return result

    async def get_sheet_rows(self, name: str):
        data = await self.aiogoogle.as_service_account(
            self.sheets.spreadsheets.values.get(
                spreadsheetId=self.spreadsheetId, range=name
            )
        )
        rows = data.get("values", [])
        return rows

    async def add_sheet_rows(self, sheet_name: str, values: List[List[Any]]):
        result = await self.aiogoogle.as_service_account(
            self.sheets.spreadsheets.values.append(
                spreadsheetId=self.spreadsheetId,
                range=sheet_name,
                valueInputOption="USER_ENTERED",
                json=dict(values=values),
            )
        )

        return result


if __name__ == "__main__":
    s = GoogleSheets(TRADING_DIARY_SPREEDSHEET_ID, GOOGLE_SERVICE_KEY_FILE_NAME)

    async def test_spreadsheets():
        await s.init()
        names = await s.get_sheet_rows("trading")
        print(names)
        await s.add_sheet_rows("trading", [[1, 2, 3]])

    asyncio.run(test_spreadsheets())
