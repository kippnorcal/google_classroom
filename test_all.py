import os
import pandas as pd
from config import TestConfig, db_generator
from api import (
    Announcements,
    CourseAliases,
    Courses,
    CourseWork,
    OrgUnits,
    Guardians,
    GuardianInvites,
    Invitations,
    Topics,
    Students,
    Teachers,
    StudentSubmissions,
    StudentUsage,
)
from tests.mock_response import FakeService
from tests.responses import (
    ALIAS_SOLUTION,
    ANNOUNCEMENT_SOLUTION,
    COURSE_SOLUTION,
    COURSEWORK_SOLUTION,
    GUARDIAN_SOLUTION,
    GUARDIAN_INVITE_SOLUTION,
    INVITATION_SOLUTION,
    ORG_UNIT_SOLUTION,
    STUDENT_SOLUTION,
    STUDENT_SUBMISSION_SOLUTION,
    STUDENT_USAGE_SOLUTION,
    TEACHER_SOLUTION,
    TOPIC_SOLUTION,
)


class TestEndToEnd:
    def setup(self):
        self.config = TestConfig
        self.sql = db_generator(self.config)
        self.service = FakeService()

    def teardown(self):
        if os.path.exists(self.config.DB):
            os.remove(self.config.DB)

    def test_get_org_units(self):
        self.generic_get_test(
            OrgUnits(self.service, self.sql, self.config), ORG_UNIT_SOLUTION
        )

    def test_get_student_usage(self):
        self.generic_get_test(
            StudentUsage(self.service, self.sql, self.config, None),
            STUDENT_USAGE_SOLUTION,
            dates=["2020-02-27", "2020-02-28"],
        )

    def test_get_partial_student_usage(self):
        self.generic_get_test(
            StudentUsage(self.service, self.sql, self.config, None),
            STUDENT_USAGE_SOLUTION.loc[
                STUDENT_USAGE_SOLUTION["AsOfDate"] == pd.to_datetime("2020-02-27")
            ],
            dates=["2020-02-27"],
        )

    def test_get_guardians(self):
        self.generic_get_test(
            Guardians(self.service, self.sql, self.config), GUARDIAN_SOLUTION
        )

    def test_get_guardian_invites(self):
        self.generic_get_test(
            GuardianInvites(self.service, self.sql, self.config),
            GUARDIAN_INVITE_SOLUTION,
        )

    def test_get_courses(self):
        self.generic_get_test(
            Courses(self.service, self.sql, self.config), COURSE_SOLUTION
        )

    def test_get_topics(self):
        self.generic_get_test(
            Topics(self.service, self.sql, self.config),
            TOPIC_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_students(self):
        self.generic_get_test(
            Students(self.service, self.sql, self.config),
            STUDENT_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_teachers(self):
        self.generic_get_test(
            Teachers(self.service, self.sql, self.config),
            TEACHER_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_aliases(self):
        self.generic_get_test(
            CourseAliases(self.service, self.sql, self.config),
            ALIAS_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_invitations(self):
        self.generic_get_test(
            Invitations(self.service, self.sql, self.config),
            INVITATION_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_announcements(self):
        self.generic_get_test(
            Announcements(self.service, self.sql, self.config),
            ANNOUNCEMENT_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_submissions(self):
        self.generic_get_test(
            StudentSubmissions(self.service, self.sql, self.config),
            STUDENT_SUBMISSION_SOLUTION,
            course_ids=[0, 1],
        )

    def test_get_coursework(self):
        self.generic_get_test(
            CourseWork(self.service, self.sql, self.config),
            COURSEWORK_SOLUTION,
            course_ids=[0, 1],
        )

    def generic_get_test(self, endpoint, solution, course_ids=[None], dates=[None]):
        endpoint.batch_pull_data(course_ids=course_ids, dates=dates)
        result = pd.read_sql_table(
            endpoint.table_name, con=self.sql.engine, schema=self.sql.schema
        )
        assert result.equals(solution)
