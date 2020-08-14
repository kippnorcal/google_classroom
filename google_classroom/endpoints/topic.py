from endpoints.base import EndPoint


class Topic(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["updateTime"]
        self.columns = ["courseId", "topicId", "name", "updateTime"]
        self.request_key = "topic"
        self.batch_size = config.TOPICS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .topics()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )
