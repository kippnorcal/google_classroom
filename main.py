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
    admin_service = build("admin", "reports_v1", credentials=creds)

    sql = MSSQL()

    # Get usage
    if args.usage:
        student_usage = StudentUsage(admin_service)
        student_usage.get()
        student_usage_df = student_usage.to_df()
        sql.insert_into("GoogleClassroom_StudentUsage", student_usage_df)

    # Get guardians
    if args.guardians:
        guardians = Guardians(classroom_service)
        guardians.get()
        guardians_df = guardians.to_df()
        sql.insert_into("GoogleClassroom_Guardians", guardians_df, if_exists="replace")

    # Get guardian invites
    if args.invites:
        guardian_invites = GuardianInvites(classroom_service)
        guardian_invites.get()
        guardian_invites_df = guardian_invites.to_df()
        sql.insert_into(
            "GoogleClassroom_GuardianInvites", guardian_invites_df, if_exists="replace"
        )

    # Get courses
    if args.courses:
        courses = Courses(classroom_service)
        courses.get()
        courses_df = courses.to_df()
        courses_df = courses_df[courses_df.updateTime >= "2019-07-01"]
        sql.insert_into("GoogleClassroom_Courses", courses_df, if_exists="replace")

    # Get list of course ids
    course_ids = sql.query("SELECT id FROM custom.GoogleClassroom_Courses")
    course_ids = course_ids.id.unique()

    # Get course topics
    if args.topics:
        topics = Topics(classroom_service)
        topics.get_by_course(course_ids)
        topics_df = topics.to_df()
        sql.insert_into("GoogleClassroom_Topics", topics_df, if_exists="replace")

    # Get CourseWork
    if args.coursework:
        course_work = CourseWork(classroom_service)
        course_work.get_by_course(course_ids)
        course_work_df = course_work.to_df()
        sql.insert_into(
            "GoogleClassroom_CourseWork", course_work_df, if_exists="replace"
        )

    # Get students and insert into database
    if args.students:
        students = Students(classroom_service)
        students.get_by_course(course_ids)
        students_df = students.to_df()
        sql.insert_into("GoogleClassroom_Students", students_df, if_exists="replace")

    # Get teachers and insert into database
    if args.teachers:
        teachers = Teachers(classroom_service)
        teachers.get_by_course(course_ids)
        teachers_df = teachers.to_df()
        sql.insert_into("GoogleClassroom_Teachers", teachers_df, if_exists="replace")

    # Get student coursework submissions
    if args.submissions:
        student_submissions = StudentSubmissions(classroom_service)
        student_submissions.get_by_course(course_ids)
        student_submissions_df = student_submissions.to_df()
        sql.insert_into(
            "GoogleClassroom_CourseworkSubmissions",
            student_submissions_df,
            if_exists="replace",
        )


if __name__ == "__main__":
    main()
