from datetime import datetime
import logging
import os
import pickle
import sys
import traceback

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd

from api import (
    Courses,
    CourseWork,
    GuardianInvites,
    Guardians,
    OrgUnits,
    Students,
    StudentSubmissions,
    StudentUsage,
    Teachers,
    Topics,
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


def get_credentials():
    """Retrieve Google auth credentials needed to build service"""
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = [
        "https://www.googleapis.com/auth/admin.directory.orgunit",
        "https://www.googleapis.com/auth/admin.reports.usage.readonly",
        "https://www.googleapis.com/auth/classroom.courses",
        "https://www.googleapis.com/auth/classroom.coursework.students",
        "https://www.googleapis.com/auth/classroom.guardianlinks.students",
        "https://www.googleapis.com/auth/classroom.profile.emails",
        "https://www.googleapis.com/auth/classroom.rosters",
        "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
        "https://www.googleapis.com/auth/classroom.topics",
    ]
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return creds


def main(config):
    configure_logging(config)
    creds = get_credentials()
    classroom_service = build("classroom", "v1", credentials=creds)
    admin_reports_service = build("admin", "reports_v1", credentials=creds)
    admin_directory_service = build("admin", "directory_v1", credentials=creds)
    sql = db_generator(config)

    # Get usage
    if config.PULL_USAGE:
        # First get student org unit
        result = OrgUnits(admin_directory_service, sql, config).batch_pull_data()
        ou_id = None if result.empty else result.iloc[0]

        # Then get usage
        # Clear out the last day's worth of data, because it may only be partially
        # complete. Then load data on all dates from that day until today.
        usage = StudentUsage(admin_reports_service, sql, config, ou_id)
        last_date = usage.get_last_date()
        if last_date:
            usage.remove_dates_after(last_date)
        start_date = last_date or datetime.strptime(
            config.SCHOOL_YEAR_START, "%Y-%m-%d"
        )
        date_range = pd.date_range(start=start_date, end=datetime.today()).strftime(
            "%Y-%m-%d"
        )
        usage.batch_pull_data(dates=date_range, overwrite=False)

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
    ):
        course_ids = Courses(classroom_service, sql, config).get_course_ids()

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


if __name__ == "__main__":
    try:
        main(Config)
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
    if not Config.DISABLE_MAILER:
        Mailer(Config, "Google Classroom Connector").notify(error_message=error_message)
