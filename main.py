from datetime import datetime, timedelta
import logging
import os
import pickle
import sys
from config import Config, db_generator
import pandas as pd
import traceback
from mailer import Mailer

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

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
    try:
        configure_logging(config)
        creds = get_credentials()
        classroom_service = build("classroom", "v1", credentials=creds)
        admin_reports_service = build("admin", "reports_v1", credentials=creds)
        admin_directory_service = build("admin", "directory_v1", credentials=creds)
        sql = db_generator(config)

        # Get usage
        if config.PULL_USAGE:
            # First get student org unit
            result = OrgUnits(
                admin_directory_service, config.STUDENT_ORG_UNIT
            ).get_and_write_to_db(sql, debug=config.DEBUG)
            ou_id = None if result.empty else result.iloc[0]

            # Then get usage
            StudentUsage(admin_reports_service, ou_id).get_and_write_to_db(
                sql, overwrite=False, debug=config.DEBUG
            )

        # Get guardians
        if config.PULL_GUARDIANS:
            Guardians(classroom_service).get_and_write_to_db(sql, debug=config.DEBUG)

        # Get guardian invites
        if config.PULL_GUARDIAN_INVITES:
            GuardianInvites(classroom_service).get_and_write_to_db(
                sql, debug=config.DEBUG
            )

        # Get courses
        if config.PULL_COURSES:
            Courses(classroom_service, config.SCHOOL_YEAR_START).get_and_write_to_db(
                sql, debug=config.DEBUG
            )

        # Get list of course ids
        if (
            config.PULL_TOPICS
            or config.PULL_COURSEWORK
            or config.PULL_STUDENTS
            or config.PULL_TEACHERS
            or config.PULL_SUBMISSIONS
        ):
            course_ids = Courses(
                classroom_service, config.SCHOOL_YEAR_START
            ).get_course_ids(sql)

        # Get course topics
        if config.PULL_TOPICS:
            Topics(classroom_service).get_and_write_to_db(
                sql, course_ids, debug=config.DEBUG
            )

        # Get CourseWork
        if config.PULL_COURSEWORK:
            CourseWork(classroom_service).get_and_write_to_db(
                sql, course_ids, debug=config.DEBUG
            )

        # Get students and insert into database
        if config.PULL_STUDENTS:
            Students(classroom_service).get_and_write_to_db(
                sql, course_ids, debug=config.DEBUG
            )

        # Get teachers and insert into database
        if config.PULL_TEACHERS:
            Teachers(classroom_service).get_and_write_to_db(
                sql, course_ids, debug=config.DEBUG
            )

        # Get student coursework submissions
        if config.PULL_SUBMISSIONS:
            StudentSubmissions(classroom_service).get_and_write_to_db(
                sql, course_ids, debug=config.DEBUG
            )

        Mailer("Google Classroom Connector").notify()

    except Exception as e:
        logging.exception(e)
        stack_trace = traceback.format_exc()
        Mailer("Google Classroom Connector").notify(error=True, message=stack_trace)


if __name__ == "__main__":
    main(Config)
