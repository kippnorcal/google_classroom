from endpoints.base import EndPoint
from datetime import timedelta
import logging


class Meet(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["item_time"]
        self.columns = [
            "conference_id",
            "device_type",
            "display_name",
            "duration_seconds",
            "endpoint_id",
            "identifier",
            "identifier_type",
            "ip_address",
            "is_external",
            "meeting_code",
            "organizer_email",
            "item_time",
            "event_name",
        ]
        self.request_key = "items"
        self.batch_size = config.MEET_BATCH_SIZE
        self.last_date = None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request Google Meet events (currently only call_ended)"""
        options = {
            "applicationName": "meet",
            "userKey": "all",
            "eventName": "call_ended",
            "pageToken": next_page_token,
        }
        # Meet data is added incrementally, because the data is very large. However,
        # the data Google provides is not always fully up-to-date. As a result, the
        # most reliable way to obtain incremental updated data is to drop the last
        # 24 hours of data and then request data from Google starting at that point.
        if next_page_token is None:
            # Only set the last_date on the first request.
            data = self.return_all_data()
            if data is not None and data.item_time.count() > 0:
                last_date = data.item_time.max()
                table = self.sql.table(self.table_name)
                last_date = last_date - timedelta(hours=24)
                delete_query = table.delete().where(table.c.item_time > last_date)
                self.sql.engine.execute(delete_query)
                last_date = last_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                logging.debug(f"{self.classname()}: pulling data from {last_date}.")
                self.last_date = last_date
            else:
                self.last_date = None
        if self.last_date:
            options["startTime"] = self.last_date

        return self.service.activities().list(**options)

    def preprocess_records(self, records):
        """Pull out parameter data from the returned Google Meet call event"""
        new_records = []
        for record in records:
            event_records = record.get("events")
            item_time = record.get("id").get("time")
            for event_record in event_records:
                event_name = event_record.get("name")
                if event_name == "call_ended":
                    new_record = {"item_time": item_time, "event_name": event_name}
                    for subrecord in event_record.get("parameters"):
                        name = subrecord.get("name")
                        value = (
                            subrecord.get("value")
                            or subrecord.get("intValue")
                            or subrecord.get("boolValue")
                        )
                        new_record[name] = value
                    new_records.append(new_record)
        return new_records
