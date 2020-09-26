from endpoints.base import EndPoint


class Students(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["courseId", "userId", "fullName", "emailAddress"]
        self.request_key = "students"
        self.batch_size = config.STUDENTS_BATCH_SIZE
        self.columns_to_merge_on = ["alias", "emailAddress"]
        self.should_delete_on_sync = True

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .students()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )

    def preprocess_records(self, records):
        for record in records:
            record["fullName"] = record.get("profile").get("name").get("fullName")
            record["emailAddress"] = record.get("profile").get("emailAddress")
        return records

    def create_new_item(self, student):
        return (
            self.service.courses()
            .students()
            .create(courseId=student["alias"], body={"userId": student["emailAddress"]})
        )

    def delete_item(self, student):
        return (
            self.service.courses()
            .students()
            .delete(courseId=student["courseId"], userId=student["userId"])
        )
