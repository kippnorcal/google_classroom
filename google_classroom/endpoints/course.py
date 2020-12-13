from endpoints.base import EndPoint


class Courses(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime", "updateTime"]
        self.columns = [
            "id",
            "name",
            "courseGroupEmail",
            "courseState",
            "creationTime",
            "description",
            "descriptionHeading",
            "enrollmentCode",
            "guardiansEnabled",
            "ownerId",
            "room",
            "section",
            "teacherGroupEmail",
            "updateTime",
            "calendarId",
        ]
        self.request_key = "courses"
        self.batch_size = config.COURSES_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return self.service.courses().list(
            pageToken=next_page_token,
            pageSize=self.config.PAGE_SIZE,
        )

    def filter_data(self, dataframe):
        return dataframe[dataframe.updateTime >= self.config.SCHOOL_YEAR_START]

    def return_cleaned_sync_data(self):
        df = self.return_all_data().astype("str")
        df = df[df["courseState"] == "ACTIVE"]
        df = df.rename(columns={"id": "courseId"})
        df = df[["courseId", "name", "section"]]
        return df.astype("str")
