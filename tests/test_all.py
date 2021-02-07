import pandas as pd
from config import TestConfig, db_generator

from endpoints import (
    Announcements,
    Courses,
    CourseAliases,
    CourseWork,
    Guardians,
    GuardianInvites,
    Invitations,
    Meet,
    OrgUnits,
    Students,
    StudentSubmissions,
    StudentUsage,
    Teachers,
    Topics,
)

from mock_response import FakeService
from responses import (
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
    MEET_SOLUTION,
)
from sync_data import (
    COURSE_DATA,
    ALIAS_DATA,
    STUDENT_DATA,
    TEACHER_DATA,
    COURSE_SYNC_DATA,
    STUDENT_SYNC_DATA,
    TEACHER_SYNC_DATA,
    TO_CREATE_COURSE_SOLUTION,
    TO_DELETE_COURSE_SOLUTION,
    TO_CREATE_STUDENT_SOLUTION,
    TO_DELETE_STUDENT_SOLUTION,
    TO_CREATE_TEACHER_SOLUTION,
    TO_DELETE_TEACHER_SOLUTION,
)


class TestPulls:
    def setup(self):
        self.config = TestConfig
        self.sql = db_generator(self.config)
        self.service = FakeService()

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

    def test_get_meet(self):
        self.generic_get_test(Meet(self.service, self.sql, self.config), MEET_SOLUTION)

    def test_get_topics(self):
        self.generic_get_test(
            Topics(self.service, self.sql, self.config),
            TOPIC_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_students(self):
        self.generic_get_test(
            Students(self.service, self.sql, self.config),
            STUDENT_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_teachers(self):
        self.generic_get_test(
            Teachers(self.service, self.sql, self.config),
            TEACHER_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_aliases(self):
        self.generic_get_test(
            CourseAliases(self.service, self.sql, self.config),
            ALIAS_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_invitations(self):
        self.generic_get_test(
            Invitations(self.service, self.sql, self.config),
            INVITATION_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_announcements(self):
        self.generic_get_test(
            Announcements(self.service, self.sql, self.config),
            ANNOUNCEMENT_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_submissions(self):
        self.generic_get_test(
            StudentSubmissions(self.service, self.sql, self.config),
            STUDENT_SUBMISSION_SOLUTION,
            course_ids=["1", "2"],
        )

    def test_get_coursework(self):
        self.generic_get_test(
            CourseWork(self.service, self.sql, self.config),
            COURSEWORK_SOLUTION,
            course_ids=["1", "2"],
        )

    def generic_get_test(self, endpoint, solution, course_ids=[None], dates=[None]):
        endpoint.batch_pull_data(course_ids=course_ids, dates=dates)
        result = pd.read_sql_table(
            endpoint.table_name, con=self.sql.engine, schema=self.sql.schema
        )
        assert result.equals(solution)
        endpoint._drop_table()


class TestSync:
    def setup(self):
        self.config = TestConfig
        self.sql = db_generator(self.config)
        self.service = FakeService()

    def test_sync_courses(self):
        courses = Courses(self.service, self.sql, self.config)
        aliases = CourseAliases(self.service, self.sql, self.config)
        self.sql.insert_into(courses.table_name, COURSE_DATA)
        self.sql.insert_into(aliases.table_name, ALIAS_DATA)
        (to_create, to_delete) = courses.sync_data(COURSE_SYNC_DATA)
        assert to_create.equals(TO_CREATE_COURSE_SOLUTION)
        assert to_delete.equals(TO_DELETE_COURSE_SOLUTION)
        courses._drop_table()
        aliases._drop_table()

    def test_sync_students(self):
        courses = Courses(self.service, self.sql, self.config)
        aliases = CourseAliases(self.service, self.sql, self.config)
        students = Students(self.service, self.sql, self.config)
        self.sql.insert_into(courses.table_name, COURSE_DATA)
        self.sql.insert_into(aliases.table_name, ALIAS_DATA)
        self.sql.insert_into(students.table_name, STUDENT_DATA)
        (to_create, to_delete) = students.sync_data(STUDENT_SYNC_DATA)
        assert to_create.equals(TO_CREATE_STUDENT_SOLUTION)
        assert to_delete.equals(TO_DELETE_STUDENT_SOLUTION)
        courses._drop_table()
        aliases._drop_table()
        students._drop_table()

    def test_sync_teachers(self):
        courses = Courses(self.service, self.sql, self.config)
        aliases = CourseAliases(self.service, self.sql, self.config)
        teachers = Teachers(self.service, self.sql, self.config)
        self.sql.insert_into(courses.table_name, COURSE_DATA)
        self.sql.insert_into(aliases.table_name, ALIAS_DATA)
        self.sql.insert_into(teachers.table_name, TEACHER_DATA)
        (to_create, to_delete) = teachers.sync_data(TEACHER_SYNC_DATA)
        assert to_create.equals(TO_CREATE_TEACHER_SOLUTION)
        assert to_delete.equals(TO_DELETE_TEACHER_SOLUTION)
        courses._drop_table()
        aliases._drop_table()
        teachers._drop_table()
