from endpoints.base import EndPoint
from sqlalchemy import text


class StudentSubmissions(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = [
            "creationTime",
            "updateTime",
            "createdTime",
            "turnedInTimestamp",
            "returnedTimestamp",
            "draftGradeTimestamp",
            "assignedGradeTimestamp",
        ]
        self.columns = [
            "courseId",
            "courseWorkId",
            "id",
            "userId",
            "creationTime",
            "updateTime",
            "state",
            "draftGrade",
            "assignedGrade",
            "courseWorkType",
            "createdTime",
            "turnedInTimestamp",
            "returnedTimestamp",
            "draftMaxPoints",
            "draftGradeTimestamp",
            "draftGraderId",
            "assignedMaxPoints",
            "assignedGradeTimestamp",
            "assignedGraderId",
            "late",
        ]
        self.request_key = "studentSubmissions"
        self.batch_size = config.SUBMISSIONS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .courseWork()
            .studentSubmissions()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                courseWorkId="-",
                pageSize=self.config.PAGE_SIZE,
            )
        )

    def _parse_state_history(self, record, parsed):
        """Flatten timestamp records from nested state history"""
        submission_history = record.get("submissionHistory")
        if submission_history:
            for submission in submission_history:
                state_history = submission.get("stateHistory")
                if state_history:
                    state = state_history.get("state")
                    if state == "CREATED":
                        parsed["createdTime"] = state_history.get("stateTimestamp")
                    elif state == "TURNED_IN":
                        parsed["turnedInTimestamp"] = state_history.get(
                            "stateTimestamp"
                        )
                    elif state == "RETURNED":
                        parsed["returnedTimestamp"] = state_history.get(
                            "stateTimestamp"
                        )

    def _parse_grade_history(self, record, parsed):
        """Flatten needed records from nested grade history"""
        submission_history = record.get("submissionHistory")
        if submission_history:
            for submission in submission_history:
                grade_history = submission.get("gradeHistory")
                if grade_history:
                    grade_change_type = grade_history.get("gradeChangeType")
                    if grade_change_type == "DRAFT_GRADE_POINTS_EARNED_CHANGE":
                        parsed["draftMaxPoints"] = grade_history.get("maxPoints")
                        parsed["draftGradeTimestamp"] = grade_history.get(
                            "gradeTimestamp"
                        )
                        parsed["draftGraderId"] = grade_history.get("actorUserId")
                    elif grade_change_type == "ASSIGNED_GRADE_POINTS_EARNED_CHANGE":
                        parsed["assignedMaxPoints"] = grade_history.get("maxPoints")
                        parsed["assignedGradeTimestamp"] = grade_history.get(
                            "gradeTimestamp"
                        )
                        parsed["assignedGraderId"] = grade_history.get("actorUserId")

    def preprocess_records(self, records):
        new_records = []
        for record in records:
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
                "late": record.get("late", False),
            }
            self._parse_state_history(record, parsed)
            self._parse_grade_history(record, parsed)
            new_records.append(parsed)
        return new_records

    def perform_cleanup(self):
        with open("sql/remove_duplicates.sql") as sql_file:
            escaped_sql = text(sql_file.read())
            with self.sql.engine.connect().execution_options(autocommit=True) as conn:
                conn.execute(escaped_sql)
