import json
from pandas import json_normalize

import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import pandas as pd
from sqlsorcery import MSSQL



def build_service():
    # If modifying these scopes, delete the file token.pickle.
SCOPES = [
    "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
    "https://www.googleapis.com/auth/classroom.courses",
    "https://www.googleapis.com/auth/classroom.rosters",
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

    service = build("classroom", "v1", credentials=creds)
    return service

def get_courses(service):
    # Get all paginated courses
    all_courses = []
    next_page_token = ""
    while next_page_token is not None:
        results = service.courses().list(pageToken=next_page_token).execute()
        courses = results.get("courses", [])
        next_page_token = results.get("nextPageToken", None)
        all_courses.extend(courses)
    return all_courses


def get_students(service, course_ids):
    # For the given courses, get the list of students. Can take a while for a large number of courses.
    all_students = []
    for course_id in course_ids:
        print(f"getting students for {course_id}")
        results = service.courses().students().list(courseId=course_id).execute()
        students = results.get("students", [])
        all_students.extend(students)
    return all_students


def get_teachers(service, course_ids):
    # For the given courses, get the list of teachers. Can take a while for a large number of courses.
    all_teachers = []
    for course_id in course_ids:
        print(f"getting teachers for {course_id}")
        results = service.courses().teachers().list(courseId=course_id).execute()
        teachers = results.get("teachers", [])
        all_teachers.extend(teachers)
    return teachers


def main():
    service = build_service()

    # coursework = (
    #     service.courses()
    #     .courseWork()
    #     .studentSubmissions()
    #     .list(courseId="41136730708", courseWorkId="-")
    #     .execute()
    # )

    # with open("coursework.json", "w", encoding="utf-8") as f:
    #     json.dump(coursework, f, ensure_ascii=False, indent=4)

    sql = MSSQL()
    # Get courses
    courses = get_courses(service)
    courses = json_normalize(courses)
    # courses = courses.astype(str)
    # sql.insert_into("GoogleClassroom_Courses", courses) # this is erroring on insertion
    course_ids = courses.id.unique()

    # Get students and insert into database
    students = get_students(service, course_ids)
    students = json_normalize(students)
    students = students.astype(str)
    sql.insert_into("GoogleClassroom_Students", students)

    # Get teachers and insert into database
    teachers = get_teachers(service, course_ids)
    teachers = json_normalize(teachers)
    teachers = teachers.astype(str)
    sql.insert_into("GoogleClassroom_Teachers", teachers)


if __name__ == "__main__":
    main()
