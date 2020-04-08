from contextlib import suppress
from datetime import datetime, timedelta
import json
import logging
import os
import pickle
import sys
from config import Config, db_generator

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
from sqlalchemy.exc import ProgrammingError

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


def get_values_and_write_to_db(
    sql, endpoint, table_name, if_exists="replace", course_ids=[]
):
    if len(course_ids) > 0:
        endpoint.get_by_course(course_ids)
    else:
        endpoint.get()
    endpoint_df = endpoint.to_df()
    write_df_to_db(sql, endpoint_df, table_name, if_exists)


def write_df_to_db(sql, dataframe, table_name, if_exists="replace"):
    full_table_name = "GoogleClassroom_" + table_name
    logging.info(f"Inserting {len(dataframe)} records into {full_table_name}.")
    if not dataframe.empty:
        sql.insert_into(full_table_name, dataframe, if_exists=if_exists)


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
        org_units = OrgUnits(admin_directory_service)
        org_units.get()
        ou_df = org_units.to_df()
        ou_series = ou_df.loc[ou_df.name == config.STUDENT_ORG_UNIT, "orgUnitId"]
        ou_id = None if ou_series.empty else ou_series.iloc[0]

        # Then get usage
        student_usage = StudentUsage(admin_reports_service, ou_id)
        get_values_and_write_to_db(
            sql, student_usage, "StudentUsage", if_exists="append"
        )

    # Get guardians
    if config.PULL_GUARDIANS:
        guardians = Guardians(classroom_service)
        get_values_and_write_to_db(sql, guardians, "Guardians")

    # Get guardian invites
    if config.PULL_GUARDIAN_INVITES:
        guardian_invites = GuardianInvites(classroom_service)
        get_values_and_write_to_db(sql, guardian_invites, "GuardianInvites")

    # Get courses
    if config.PULL_COURSES:
        courses = Courses(classroom_service)
        courses.get()
        courses_df = courses.to_df()
        courses_df = courses_df[courses_df.updateTime >= config.SCHOOL_YEAR_START]
        write_df_to_db(sql, courses_df, "Courses", if_exists="replace")

    # Get list of course ids
    if (
        config.PULL_TOPICS
        or config.PULL_COURSEWORK
        or config.PULL_STUDENTS
        or config.PULL_TEACHERS
        or config.PULL_SUBMISSIONS
    ):
        courses = pd.read_sql_table(
            "GoogleClassroom_Courses", con=sql.engine, schema=sql.schema
        )
        course_ids = courses.id.unique()

    # Get course topics
    if config.PULL_TOPICS:
        topics = Topics(classroom_service)
        get_values_and_write_to_db(sql, topics, "Topics", course_ids=course_ids)

    # Get CourseWork
    if config.PULL_COURSEWORK:
        course_work = CourseWork(classroom_service)
        get_values_and_write_to_db(
            sql, course_work, "CourseWork", course_ids=course_ids
        )

    # Get students and insert into database
    if config.PULL_STUDENTS:
        students = Students(classroom_service)
        get_values_and_write_to_db(sql, students, "Students", course_ids=course_ids)

    # Get teachers and insert into database
    if config.PULL_TEACHERS:
        teachers = Teachers(classroom_service)
        get_values_and_write_to_db(sql, teachers, "Teachers", course_ids=course_ids)

    # Get student coursework submissions
    if config.PULL_SUBMISSIONS:
        student_submissions = StudentSubmissions(classroom_service)
        get_values_and_write_to_db(
            sql, student_submissions, "CourseworkSubmissions", course_ids=course_ids
        )


if __name__ == "__main__":
    main(Config)
