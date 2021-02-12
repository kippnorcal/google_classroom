import logging
import pandas as pd
from datetime import datetime

from endpoints.base import EndPoint


class StudentUsage(EndPoint):
    def __init__(self, service, sql, config, org_unit_id):
        super().__init__(service, sql, config)
        self.date_columns = ["AsOfDate", "LastUsedTime", "ImportDate"]
        self.columns = ["Email", "AsOfDate", "LastUsedTime", "ImportDate"]
        self.org_unit_id = org_unit_id
        self.request_key = "usageReports"
        self.batch_size = config.USAGE_BATCH_SIZE

    def get_last_date(self):
        """Gets the last available date of data in the database."""
        try:
            usage = pd.read_sql_table(
                self.table_name, con=self.sql.engine, schema=self.sql.schema
            )
            return usage.AsOfDate.max() if usage.AsOfDate.count() > 0 else None
        except ValueError:
            # Table doesn't yet exist.
            return None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        options = {
            "userKey": "all",
            "date": date,
            "pageToken": next_page_token,
            "parameters": "classroom:last_interaction_time",
        }
        if self.org_unit_id:
            options["orgUnitID"] = self.org_unit_id
        return self.service.userUsageReport().get(**options)

    def preprocess_records(self, records):
        new_records = []
        for record in records:
            row = {}
            row["Email"] = record.get("entity").get("userEmail")
            row["AsOfDate"] = record.get("date")
            row["LastUsedTime"] = self._get_date_from_record(record)
            row["ImportDate"] = datetime.today().strftime("%Y-%m-%d")
            new_records.append(row)
        return new_records

    def _get_date_from_record(self, record):
        if "parameters" not in record:
            logging.debug(f"{self.classname()}: Parameters not in record {record}.")
        parameters = record.get("parameters", [])
        for parameter in parameters:
            return parameter.get("datetimeValue")
        return "1970-01-01T00:00:00.00Z"
