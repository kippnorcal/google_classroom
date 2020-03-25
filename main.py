import datetime as dt
import json

import pickle
import os.path
from os import getenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import pandas as pd
from pandas import json_normalize
from sqlsorcery import MSSQL


def build_service():
    # If modifying these scopes, delete the file token.pickle.
    SCOPES = [
        "https://www.googleapis.com/auth/classroom.student-submissions.students.readonly",
        "https://www.googleapis.com/auth/classroom.courses",
        "https://www.googleapis.com/auth/classroom.topics",
        "https://www.googleapis.com/auth/classroom.rosters",
        "https://www.googleapis.com/auth/admin.reports.usage.readonly",
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

    classroom_service = build("classroom", "v1", credentials=creds)
    admin_service = build("admin", "reports_v1", credentials=creds)
    return classroom_service, admin_service


def get_classroom_student_usage(sql, service):
    """Get paginated student usage data for Google Classroom and insert into database."""
    two_days_ago = dt.datetime.today() - dt.timedelta(days=2)  # Analytics has 2 day lag
    two_days_ago = two_days_ago.strftime("%Y-%m-%d")
    print(f"Getting student usage data for {two_days_ago}.")
    all_usage = []
    next_page_token = ""
    org_unit_id = getenv("STUDENT_ORG_UNIT")
    while next_page_token is not None:
        results = (
            service.userUsageReport()
            .get(
                userKey="all",
                date=two_days_ago,
                orgUnitID=f"id:{org_unit_id}",
                pageToken=next_page_token,
            )
            .execute()
        )
        usage_data = results.get("usageReports")
        df = parse_classroom_usage(usage_data)
        sql.insert_into("GoogleClassroom_StudentUsage", df, if_exists="append")
        next_page_token = results.get("nextPageToken")


def parse_classroom_usage(usage_data):
    """Parse classroom usage data into a dataframe with one row per user."""
    records = []
    for record in usage_data:
        row = {}
        row["Email"] = record.get("entity").get("userEmail")
        row["AsOfDate"] = record.get("date")
        row["LastUsedTime"] = parse_classroom_last_used(record.get("parameters"))
        records.append(row)
    return pd.DataFrame(records)


def parse_classroom_last_used(parameters):
    """Get classroom last interaction time from parameters list."""
    for parameter in parameters:
        if parameter.get("name") == "classroom:last_interaction_time":
            return parameter.get("datetimeValue")


def get_courses(service):
    """Get all paginated courses"""
    all_courses = []
    next_page_token = ""
    while next_page_token is not None:
        results = service.courses().list(pageToken=next_page_token).execute()
        courses = results.get("courses", [])
        next_page_token = results.get("nextPageToken", None)
        all_courses.extend(courses)
    return all_courses


def get_course_topics(service, course_ids):
    """Get all course topics"""
    all_course_topics = []
    for course_id in course_ids:
        print(f"getting course topics for {course_id}")
        results = service.courses().topics().list(courseId=course_id).execute()
        topics = results.get("topic", [])
        all_course_topics.extend(topics)
    return all_course_topics


def get_students(service, course_ids):
    """For the given courses, get the list of students. Can take a while for a large number of courses."""
    all_students = []
    for course_id in course_ids:
        print(f"getting students for {course_id}")
        results = service.courses().students().list(courseId=course_id).execute()
        students = results.get("students", [])
        all_students.extend(students)
    return all_students


def get_teachers(service, course_ids):
    """For the given courses, get the list of teachers. Can take a while for a large number of courses."""
    all_teachers = []
    for course_id in course_ids:
        print(f"getting teachers for {course_id}")
        results = service.courses().teachers().list(courseId=course_id).execute()
        teachers = results.get("teachers", [])
        all_teachers.extend(teachers)
    return all_teachers


def get_student_submissions(service, course_ids):
    """For the given courses, get the list of student coursework submissions."""
    all_submissions = []
    for course_id in course_ids:
        print(f"getting student submissions for {course_id}")
        results = (
            service.courses()
            .courseWork()
            .studentSubmissions()
            .list(courseId=course_id, courseWorkId="-")
            .execute()
        )
        student_submissions = results.get("studentSubmissions", [])
        all_submissions.extend(student_submissions)
    return all_submissions


def parse_statehistory(record, parsed):
    submission_history = record.get("submissionHistory")
    if submission_history:
        for submission in submission_history:
            state_history = submission.get("stateHistory")
            if state_history:
                state = state_history.get("state")
                if state == "CREATED":
                    parsed["createdTime"] = state_history.get("stateTimestamp")
                elif state == "TURNED_IN":
                    parsed["turnedInTimestamp"] = state_history.get("stateTimestamp")
                elif state == "RETURNED":
                    parsed["returnedTimestamp"] = state_history.get("stateTimestamp")


def parse_gradehistory(record, parsed):
    submission_history = record.get("submissionHistory")
    if submission_history:
        for submission in submission_history:
            grade_history = submission.get("gradeHistory")
            if grade_history:
                grade_change_type = grade_history.get("gradeChangeType")
                if grade_change_type == "DRAFT_GRADE_POINTS_EARNED_CHANGE":
                    parsed["draftMaxPoints"] = grade_history.get("maxPoints")
                    parsed["draftGradeTimestamp"] = grade_history.get("gradeTimestamp")
                    parsed["draftGraderId"] = grade_history.get("actorUserId")
                elif grade_change_type == "ASSIGNED_GRADE_POINTS_EARNED_CHANGE":
                    parsed["assignedMaxPoints"] = grade_history.get("maxPoints")
                    parsed["assignedGradeTimestamp"] = grade_history.get(
                        "gradeTimestamp"
                    )
                    parsed["assignedGraderId"] = grade_history.get("actorUserId")


def parse_coursework(coursework):
    records = []
    for record in coursework:
        parsed = {
            "courseId": record.get("courseId"),
            "courseWorkId": record.get("courseWorkId"),
            "id": record.get("id"),
            "userId": record.get("userId"),
            "creationTime": record.get("creationTime"),
            "updateTime": record.get("updateTime"),
            "state": record.get("state"),
            "draftGrade": record.get("draftGrade"),
            "assignedGrade": record.get("assignedGrade"),
            "courseWorkType": record.get("courseWorkType"),
        }
        parse_statehistory(record, parsed)
        parse_gradehistory(record, parsed)
        records.append(parsed)
    return records


def main():
    classroom_service, admin_service = build_service()
    sql = MSSQL()

    # Get usage
    get_classroom_student_usage(sql, admin_service)

    # Get courses
    courses = get_courses(classroom_service)
    courses = json_normalize(courses)
    courses = courses.astype(str)
    sql.insert_into("GoogleClassroom_Courses", courses, if_exists="replace")
    course_ids = courses.id.unique()

    # Get course topics
    course_topics = get_course_topics(classroom_service, course_ids)
    course_topics = json_normalize(course_topics)
    course_topics = course_topics.astype(str)
    sql.insert_into("GoogleClassroom_CourseTopics", course_topics, if_exists="replace")

    # Get students and insert into database
    students = get_students(classroom_service, course_ids)
    students = json_normalize(students)
    students = students.astype(str)
    sql.insert_into("GoogleClassroom_Students", students, if_exists="replace")

    # Get teachers and insert into database
    teachers = get_teachers(classroom_service, course_ids)
    teachers = json_normalize(teachers)
    teachers = teachers.astype(str)
    sql.insert_into("GoogleClassroom_Teachers", teachers, if_exists="replace")

    # Get student coursework submissions
    student_submissions = get_student_submissions(classroom_service, course_ids)
    student_submissions = parse_coursework(student_submissions)
    student_submissions = pd.DataFrame(student_submissions)
    sql.insert_into(
        "GoogleClassroom_Coursework", student_submissions, if_exists="replace"
    )


if __name__ == "__main__":
    main()
