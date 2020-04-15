from config import TestConfig, db_generator
from api import (
    Courses,
    OrgUnits,
    Guardians,
    GuardianInvites,
    Topics,
    Students,
    Teachers,
)
import os
import pandas as pd
from test_responses import (
    FakeService,
    ORG_UNIT_SOLUTION,
    GUARDIAN_SOLUTION,
    GUARDIAN_INVITE_SOLUTION,
    COURSE_SOLUTION,
    TOPIC_SOLUTION,
    STUDENT_SOLUTION,
    TEACHER_SOLUTION,
)

# TODO: Add tests for Coursework and Submissions.


class TestEndToEnd:
    def setup(self):
        self.config = TestConfig
        self.sql = db_generator(self.config)
        self.service = FakeService()

    def teardown(self):
        if os.path.exists(self.config.SQLITE_FILE):
            os.remove(self.config.SQLITE_FILE)

    def test_get_org_units(self):
        self.generic_get_test(
            OrgUnits(self.service, self.config.STUDENT_ORG_UNIT), ORG_UNIT_SOLUTION,
        )

    def test_get_guardians(self):
        self.generic_get_test(Guardians(self.service), GUARDIAN_SOLUTION)

    def test_get_guardian_invites(self):
        self.generic_get_test(GuardianInvites(self.service), GUARDIAN_INVITE_SOLUTION)

    def test_get_courses(self):
        self.generic_get_test(
            Courses(self.service, self.config.SCHOOL_YEAR_START), COURSE_SOLUTION,
        )

    def test_get_topics(self):
        self.generic_get_test(
            Topics(self.service), TOPIC_SOLUTION, course_ids=[0, 1],
        )

    def test_get_students(self):
        self.generic_get_test(
            Students(self.service), STUDENT_SOLUTION, course_ids=[0, 1],
        )

    def test_get_teachers(self):
        self.generic_get_test(
            Teachers(self.service), TEACHER_SOLUTION, course_ids=[0, 1],
        )

    def generic_get_test(self, endpoint, solution, course_ids=[None]):
        endpoint.batch_pull_data(
            self.sql, debug=self.config.DEBUG, course_ids=course_ids
        )
        result = pd.read_sql_table(
            endpoint.table_name, con=self.sql.engine, schema=self.sql.schema
        )
        assert result.equals(solution)
