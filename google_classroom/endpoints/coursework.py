from datetime import datetime

from endpoints.base import EndPoint


class CourseWork(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime", "updateTime", "dueDate"]
        self.columns = [
            "courseId",
            "id",
            "title",
            "description",
            "state",
            "alternateLink",
            "creationTime",
            "updateTime",
            "dueDate",
            "maxPoints",
            "workType",
            "assigneeMode",
            "submissionModificationMode",
            "creatorUserId",
            "topicId",
        ]
        self.request_key = "courseWork"
        self.batch_size = config.COURSEWORK_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .courseWork()
            .list(
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
                pageToken=next_page_token,
            )
        )

    def preprocess_records(self, records):
        for record in records:
            if "dueDate" in record:
                year = record["dueDate"]["year"]
                month = record["dueDate"]["month"]
                day = record["dueDate"]["day"]
                if "dueTime" in record:
                    hours = record["dueTime"].get("hours", 0)
                    minutes = record["dueTime"].get("minutes", 0)
                    record["dueDate"] = datetime(year, month, day, hours, minutes)
                else:
                    record["dueDate"] = datetime(year, month, day)
        return records
