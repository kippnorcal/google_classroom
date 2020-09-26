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
        ]
        self.request_key = "courses"
        self.batch_size = config.COURSES_BATCH_SIZE
        self.columns_to_merge_on = ["alias", "name", "section"]
        self.should_delete_on_sync = False

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return self.service.courses().list(
            pageToken=next_page_token,
            pageSize=self.config.PAGE_SIZE,
        )

    def filter_data(self, dataframe):
        return dataframe[dataframe.updateTime >= self.config.SCHOOL_YEAR_START]

    def return_cleaned_sync_data(self):
        df = self.return_all_data().astype("str")
        df = df[df["courseState"].isin(["ACTIVE", "PROVISIONED"])]
        df = df.rename(columns={"id": "courseId"})
        df = df[["courseId", "name", "section"]]
        return df.astype("str")

    def create_new_item(self, course):
        course["courseState"] = "ACTIVE"
        course["ownerId"] = course.pop("teacher_email")
        course["id"] = course.pop("alias")
        return self.service.courses().create(body=course)
