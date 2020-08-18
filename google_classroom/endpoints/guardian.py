from endpoints.base import EndPoint


class Guardians(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["studentId", "guardianId", "invitedEmailAddress"]
        self.request_key = "guardians"
        self.batch_size = config.GUARDIANS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.userProfiles()
            .guardians()
            .list(
                studentId="-", pageToken=next_page_token, pageSize=self.config.PAGE_SIZE
            )
        )
