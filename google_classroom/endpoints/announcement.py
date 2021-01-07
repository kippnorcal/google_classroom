from endpoints.base import EndPoint


class Announcements(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime", "updateTime", "scheduledTime"]
        self.columns = [
            "id",
            "courseId",
            "text",
            "state",
            "alternateLink",
            "creationTime",
            "updateTime",
            "scheduledTime",
            "assigneeMode",
            "creatorUserId",
        ]
        self.request_key = "announcements"
        self.batch_size = config.ANNOUNCEMENTS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .announcements()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )

    def preprocess_records(self, records):
        for record in records:
            if "text" in record:
                record["text"] = record["text"].replace("\x00", "")
        return records
