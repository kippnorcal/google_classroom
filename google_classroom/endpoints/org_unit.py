from endpoints.base import EndPoint


class OrgUnits(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["name", "description", "orgUnitPath", "orgUnitId"]
        self.request_key = "organizationUnits"
        self.batch_size = config.ORG_UNIT_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request org unit that matches the given path"""
        return self.service.orgunits().list(customerId="my_customer")

    def filter_data(self, dataframe):
        return dataframe.loc[dataframe.name == self.config.STUDENT_ORG_UNIT]
