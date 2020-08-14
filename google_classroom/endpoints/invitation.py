from endpoints.base import EndPoint


class Invitation(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["id", "userId", "courseId", "role"]
        self.request_key = "invitations"
        self.batch_size = config.INVITATIONS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return self.service.invitations().list(
            pageToken=next_page_token,
            courseId=course_id,
            pageSize=self.config.PAGE_SIZE,
        )
