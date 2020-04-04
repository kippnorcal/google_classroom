import argparse
from contextlib import suppress
from datetime import datetime, timedelta
import json
import logging
import os
import pickle
import sys

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
from sqlalchemy.exc import ProgrammingError
from sqlsorcery import MSSQL

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

parser = argparse.ArgumentParser(description="Pick which ones")
parser.add_argument("--usage", help="Import student usage data", action="store_true")
parser.add_argument("--courses", help="Import course lists", action="store_true")
parser.add_argument("--topics", help="Import course topics", action="store_true")
parser.add_argument(
    "--coursework", help="Import course assignments", action="store_true"
)
parser.add_argument("--students", help="Import student rosters", action="store_true")
parser.add_argument("--teachers", help="Import teacher rosters", action="store_true")
parser.add_argument("--guardians", help="Import student guardians", action="store_true")
parser.add_argument(
    "--submissions", help="Import student coursework submissions", action="store_true"
)
parser.add_argument(
    "--invites", help="Import guardian invite statuses", action="store_true"
)
args = parser.parse_args()

logging.basicConfig(
    handlers=[
        logging.FileHandler(filename="data/app.log", mode="w+"),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.DEBUG,
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


def main():
    creds = get_credentials()
    classroom_service = build("classroom", "v1", credentials=creds)
    admin_reports_service = build("admin", "reports_v1", credentials=creds)
    admin_directory_service = build("admin", "directory_v1", credentials=creds)
    sql = MSSQL()

    def get_values_and_write_to_db(endpoint, description, table_name, if_exists="replace",
                                   course_ids=[]):
        if len(course_ids) > 0:
            endpoint.get_by_course(course_ids)
        else:
            endpoint.get()
        endpoint_df = endpoint.to_df()
        logging.info(f"Inserting {len(endpoint_df)} {description} records.")
        if not endpoint_df.empty:
            sql.insert_into(table_name, endpoint_df, if_exists=if_exists)

    # Get usage
    if args.usage:
        # First get student org unit
        org_units = OrgUnits(admin_directory_service)
        org_units.get()
        ou_df = org_units.to_df()
        ou_series = ou_df.loc[
            ou_df.name == os.getenv("STUDENT_ORG_UNIT"), "orgUnitId"
        ]
        ou_id = None if ou_series.empty else ou_series.iloc[0]

        # Then get usage
        student_usage = StudentUsage(admin_reports_service, ou_id)
        get_values_and_write_to_db(student_usage, "Student Usage",
                                   "GoogleClassroom_StudentUsage", if_exists="append")

    # Get guardians
    if args.guardians:
        guardians = Guardians(classroom_service)
        get_values_and_write_to_db(guardians, "Guardian", "GoogleClassroom_Guardians")

    # Get guardian invites
    if args.invites:
        guardian_invites = GuardianInvites(classroom_service)
        get_values_and_write_to_db(guardian_invites, "Guardian Invite",
                                   "GoogleClassroom_GuardianInvites")

    # Get courses
    if args.courses:
        courses = Courses(classroom_service)
        courses.get()
        courses_df = courses.to_df()
        courses_df = courses_df[courses_df.updateTime >= os.getenv("SCHOOL_YEAR_START")]
        logging.info(f"Inserting {len(courses_df)} Course records.")
        if not courses_df.empty:
            sql.insert_into("GoogleClassroom_Courses", courses_df, if_exists="replace")

    # Get list of course ids
    course_ids = sql.query("SELECT id FROM \"GoogleClassroom_Courses\"")
    course_ids = course_ids.id.unique()

    # Get course topics
    if args.topics:
        topics = Topics(classroom_service)
        get_values_and_write_to_db(topics, "Course Topic",
                                   "GoogleClassroom_Topics", course_ids=course_ids)

    # Get CourseWork
    if args.coursework:
        course_work = CourseWork(classroom_service)
        get_values_and_write_to_db(course_work, "Coursework",
                                   "GoogleClassroom_CourseWork", course_ids=course_ids)

    # Get students and insert into database
    if args.students:
        students = Students(classroom_service)
        get_values_and_write_to_db(
            students, "Student", "GoogleClassroom_Students", course_ids=course_ids)

    # Get teachers and insert into database
    if args.teachers:
        teachers = Teachers(classroom_service)
        get_values_and_write_to_db(
            teachers, "Teacher", "GoogleClassroom_Teachers", course_ids=course_ids)

    # Get student coursework submissions
    if args.submissions:
        student_submissions = StudentSubmissions(classroom_service)
        get_values_and_write_to_db(student_submissions, "Student Submission",
                                   "GoogleClassroom_CourseworkSubmissions", course_ids=course_ids)


if __name__ == "__main__":
    main()
