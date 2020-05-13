from datetime import datetime, timedelta
import logging
import sys
import traceback

from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd

from api import (
    Announcements,
    Courses,
    CourseWork,
    CourseAliases,
    GuardianInvites,
    Guardians,
    Invitations,
    OrgUnits,
    Students,
    StudentSubmissions,
    StudentUsage,
    Teachers,
    Topics,
    Meet,
)
from config import Config, db_generator
from mailer import Mailer


def configure_logging(config):
    logging.basicConfig(
        handlers=[
            logging.FileHandler(filename="data/app.log", mode="w+"),
            logging.StreamHandler(sys.stdout),
        ],
        level=logging.DEBUG if config.DEBUG else logging.INFO,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S%p %Z",
    )
    logging.getLogger("google_auth_oauthlib").setLevel(logging.ERROR)
    logging.getLogger("googleapiclient").setLevel(logging.ERROR)
    logging.getLogger("google").setLevel(logging.ERROR)


def get_credentials(config):
    """Generate service account credentials object"""
    SCOPES = [
        "https://www.googleapis.com/auth/classroom.announcements",
        "https://www.googleapis.com/auth/admin.directory.orgunit",
        "https://www.googleapis.com/auth/admin.reports.usage.readonly",
        "https://www.googleapis.com/auth/classroom.courses",
        "https://www.googleapis.com/auth/classroom.coursework.students",
        "https://www.googleapis.com/auth/classroom.guardianlinks.students",
        "https://www.googleapis.com/auth/classroom.profile.emails",
        "https://www.googleapis.com/auth/classroom.rosters",
        "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
        "https://www.googleapis.com/auth/classroom.topics",
        "https://www.googleapis.com/auth/admin.reports.audit.readonly",
    ]
    return service_account.Credentials.from_service_account_file(
        "service.json", scopes=SCOPES, subject=config.ACCOUNT_EMAIL
    )


def main(config):
    configure_logging(config)
    creds = get_credentials(config)
    classroom_service = build("classroom", "v1", credentials=creds)
    admin_reports_service = build("admin", "reports_v1", credentials=creds)
    admin_directory_service = build("admin", "directory_v1", credentials=creds)
    sql = db_generator(config)

    # Get usage
    if config.PULL_USAGE:
        # First get student org unit
        orgUnits = OrgUnits(admin_directory_service, sql, config)
        orgUnits.batch_pull_data()
        result = orgUnits.return_all_data()
        org_unit_id = None if result.empty else result.iloc[0].loc["orgUnitId"]

        # Then get usage, loading data from after the last available day.
        usage = StudentUsage(admin_reports_service, sql, config, org_unit_id)
        last_date = usage.get_last_date()
        if last_date:
            start_date = last_date + timedelta(days=1)
        else:
            start_date = datetime.strptime(config.SCHOOL_YEAR_START, "%Y-%m-%d")
        date_range = pd.date_range(start=start_date, end=datetime.today())
        date_range_string = date_range.strftime("%Y-%m-%d")
        usage.batch_pull_data(dates=date_range_string, overwrite=False)

    # Get guardians
    if config.PULL_GUARDIANS:
        Guardians(classroom_service, sql, config).batch_pull_data()

    # Get guardian invites
    if config.PULL_GUARDIAN_INVITES:
        GuardianInvites(classroom_service, sql, config).batch_pull_data()

    # Get courses
    if config.PULL_COURSES:
        Courses(classroom_service, sql, config).batch_pull_data()

    # Get list of course ids
    if (
        config.PULL_TOPICS
        or config.PULL_COURSEWORK
        or config.PULL_STUDENTS
        or config.PULL_TEACHERS
        or config.PULL_SUBMISSIONS
        or config.PULL_ALIASES
        or config.PULL_INVITATIONS
        or config.PULL_ANNOUNCEMENTS
    ):
        courses = Courses(classroom_service, sql, config).return_all_data()
        course_ids = courses.id.unique()

    # Get course invitations
    if config.PULL_INVITATIONS:
        Invitations(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get course announcements
    if config.PULL_ANNOUNCEMENTS:
        Announcements(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get course aliases
    if config.PULL_ALIASES:
        CourseAliases(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get course invitations
    if config.PULL_INVITATIONS:
        Invitations(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get course announcements
    if config.PULL_ANNOUNCEMENTS:
        Announcements(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get course topics
    if config.PULL_TOPICS:
        Topics(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get CourseWork
    if config.PULL_COURSEWORK:
        CourseWork(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get students and insert into database
    if config.PULL_STUDENTS:
        Students(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get teachers and insert into database
    if config.PULL_TEACHERS:
        Teachers(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get student coursework submissions
    if config.PULL_SUBMISSIONS:
        StudentSubmissions(classroom_service, sql, config).batch_pull_data(course_ids)

    # Get Meet data
    if config.PULL_MEET:
        Meet(admin_reports_service, sql, config).batch_pull_data()


if __name__ == "__main__":
    try:
        main(Config)
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if not Config.DISABLE_MAILER:
        Mailer(Config, "Google Classroom Connector").notify(error_message=error_message)
