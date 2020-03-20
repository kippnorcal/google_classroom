import pandas as pd
import json
from sqlsorcery import MSSQL


def read_json():
    with open("coursework.json") as f:
        coursework = json.loads(f.read())
    return coursework["studentSubmissions"]


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
    cw = read_json()
    parsed_records = parse_coursework(cw)
    df = pd.DataFrame(parsed_records)
    print(df)
    sql = MSSQL(schema="custom")
    sql.insert_into("GoogleClassroom_Coursework", df, if_exists="replace")


if __name__ == "__main__":
    main()
