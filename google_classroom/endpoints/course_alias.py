from endpoints.base import EndPoint


class CourseAliases(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["courseId", "alias"]
        self.request_key = "aliases"
        self.batch_size = config.ALIASES_BATCH_SIZE
        self.inject_course_id = True

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .aliases()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )
