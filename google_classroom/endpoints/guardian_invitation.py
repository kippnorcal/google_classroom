from endpoints.base import EndPoint


class GuardianInvitation(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime"]
        self.columns = [
            "studentId",
            "invitationId",
            "invitedEmailAddress",
            "state",
            "creationTime",
        ]
        self.request_key = "guardianInvitations"
        self.batch_size = config.GUARDIAN_INVITES_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.userProfiles()
            .guardianInvitations()
            .list(
                studentId="-",
                states=["PENDING", "COMPLETE"],
                pageToken=next_page_token,
                pageSize=self.config.PAGE_SIZE,
            )
        )
