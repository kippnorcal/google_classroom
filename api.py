from datetime import datetime, timedelta
import json
import logging
import os

from googleapiclient.http import BatchHttpRequest
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from timer import elapsed


class EndPoint:
    @classmethod
    def classname(cls):
        return cls.__name__

    def __init__(self, service):
        self.service = service
        self.filename = f"data/{self.classname().lower()}.json"
        self.columns = []
        self.date_columns = []
        self.course_id = None
        self.request_key = self._request_key()

    def request(self):
        pass

    def _request_key(self):
        """Convert the classname to the request key. Used to parse json response."""
        key = self.classname()
        return f"{key[0].lower()}{key[1:]}"

    def to_json(self, records):
        """Write the records to the json file. Extend the file if it already exists."""
        if os.path.exists(self.filename):
            with open(self.filename, "r+") as f:
                data = json.load(f)
                data.extend(records)
                f.seek(0)
                json.dump(data, f)
        else:
            with open(self.filename, "w") as f:
                json.dump(records, f)

    def to_df(self):
        """Convert the json file for this endpoint to a dataframe and trim for data warehouse insertion."""
        try:
            with open(self.filename) as f:
                data = json.load(f)
            df = pd.json_normalize(data)
            df = df.reindex(columns=self.columns)
            if self.date_columns:
                date_types = {col: "datetime64[ns]" for col in self.date_columns}
                df = df.astype(date_types)
            return df
        except FileNotFoundError:
            logging.warning(
                f"Unable to open {self.filename} for read access, as it does not exist.")
            return pd.DataFrame()

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get(self, course_id=None, position=None):
        """Execute API request and write results to json file."""
        self.next_page_token = ""
        self.count = 0
        while self.next_page_token is not None:
            results = self.request().execute()
            records = results.get(self.request_key, [])
            self.count += len(records)
            self.next_page_token = results.get("nextPageToken", None)
            if len(records) > 0:
                if course_id:
                    logging.debug(
                        f"Getting {self.count} {self.classname()} for course {course_id} | {position[0]}/{position[1]}"
                    )
                else:
                    logging.debug(f"Getting {self.count} {self.classname()}")

                self.to_json(records)

    @elapsed
    def get_by_course(self, course_ids):
        """Loop through a list of course IDs and get results for each course."""
        course_count = len(course_ids)
        for idx, course_id in enumerate(course_ids):
            self.course_id = course_id
            self.get(course_id=course_id, position=(idx, course_count))


class StudentUsage(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["AsOfDate", "LastUsedTime"]
        self.columns = ["Email", "AsOfDate", "LastUsedTime"]
        self.two_days_ago = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        self.org_unit_id = os.getenv("STUDENT_ORG_UNIT")

    def request(self):
        """Request all usage for the given org unit."""
        options = {
            "userKey": "all",
            "date": self.two_days_ago,
            "pageToken": self.next_page_token,
        }
        if self.org_unit_id:
            options["orgUnitId"] = self.org_unit_id  # This is the CleverStudents org unit ID

        return self.service.userUsageReport().get(**options)

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @elapsed
    def get(self, position=None):
        """Get all student usage and parse Google Classroom usage."""
        self.next_page_token = ""
        self.count = 0
        while self.next_page_token is not None:
            results = self.request().execute()
            records = results.get("usageReports")
            records = self._parse_classroom_usage(records)
            self.count += len(records)
            self.next_page_token = results.get("nextPageToken", None)
            if len(records) > 0:
                logging.debug(f"Getting {self.count} {self.classname()}")
                self.to_json(records)

    def _parse_classroom_usage(self, usage_data):
        """Parse classroom usage data into a dataframe with one row per user."""
        records = []
        for record in usage_data:
            row = {}
            row["Email"] = record.get("entity").get("userEmail")
            row["AsOfDate"] = record.get("date")
            row["LastUsedTime"] = self._parse_classroom_last_used(
                record.get("parameters")
            )
            row["ImportDate"] = datetime.today().strftime("%Y-%m-%d")
            records.append(row)
        return records

    def _parse_classroom_last_used(self, parameters):
        """Get classroom last interaction time from parameters list."""
        for parameter in parameters:
            if parameter.get("name") == "classroom:last_interaction_time":
                return parameter.get("datetimeValue")


class Guardians(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = []
        self.columns = ["studentId", "guardianId", "invitedEmailAddress"]

    def request(self):
        """Request all guardians."""
        return (
            self.service.userProfiles()
            .guardians()
            .list(studentId="-", pageToken=self.next_page_token)
        )


class GuardianInvites(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["creationTime"]
        self.columns = [
            "studentId",
            "invitationId",
            "invitedEmailAddress",
            "state",
            "creationTime",
        ]
        self.request_key = "guardianInvitations"

    def request(self):
        """Request all pending and complete guardian invites."""
        return (
            self.service.userProfiles()
            .guardianInvitations()
            .list(
                studentId="-",
                states=["PENDING", "COMPLETE"],
                pageToken=self.next_page_token,
            )
        )


class Courses(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["creationTime", "updateTime"]
        self.columns = [
            "id",
            "name",
            "courseGroupEmail",
            "courseState",
            "creationTime",
            "description",
            "descriptionHeading",
            "enrollmentCode",
            "guardiansEnabled",
            "ownerId",
            "room",
            "section",
            "teacherGroupEmail",
            "updateTime",
        ]

    def request(self, course_id=None):
        """Request all active courses."""
        return self.service.courses().list(
            pageToken=self.next_page_token, courseStates=["ACTIVE"]
        )


class Topics(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["updateTime"]
        self.columns = ["courseId", "topicId", "name", "updateTime"]
        self.request_key = "topic"

    def request(self):
        """Request all topics for this course."""
        return (
            self.service.courses()
            .topics()
            .list(pageToken=self.next_page_token, courseId=self.course_id)
        )


class Teachers(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.columns = [
            "courseId",
            "userId",
            "profile.name.fullName",
            "profile.emailAddress",
        ]

    def request(self):
        """Request all teachers for this course."""
        return (
            self.service.courses()
            .teachers()
            .list(pageToken=self.next_page_token, courseId=self.course_id)
        )


class Students(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.columns = [
            "courseId",
            "userId",
            "profile.name.fullName",
            "profile.emailAddress",
        ]

    def request(self):
        """Request all students for this course."""
        return (
            self.service.courses()
            .students()
            .list(pageToken=self.next_page_token, courseId=self.course_id)
        )


class CourseWork(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["creationTime", "updateTime"]
        self.columns = [
            "courseId",
            "id",
            "title",
            "description",
            "state",
            "alternateLink",
            "creationTime",
            "updateTime",
            "dueDate.year",
            "dueDate.month",
            "dueDate.day",
            "dueTime.hours",
            "dueTime.minutes",
            "maxPoints",
            "workType",
            "assigneeMode",
            "submissionModificationMode",
            "creatorUserId",
            "topicId",
        ]

    def request(self):
        """Request all coursework for this course."""
        return (
            self.service.courses()
            .courseWork()
            .list(pageToken=self.next_page_token, courseId=self.course_id)
        )


class StudentSubmissions(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["creationTime", "updateTime"]
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
        ]

    def request(self):
        """Request all student submissions for this course."""
        return (
            self.service.courses()
            .courseWork()
            .studentSubmissions()
            .list(courseId=self.course_id, courseWorkId="-")
        )

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get(self, course_id=None, position=None):
        """Get student submissions and parse coursework from within the submission."""
        self.next_page_token = ""
        self.count = 0
        while self.next_page_token is not None:
            results = self.request().execute()
            records = results.get(self.request_key, [])
            records = self._parse_coursework(records)
            self.count += len(records)
            self.next_page_token = results.get("nextPageToken", None)
            if len(records) > 0:
                if course_id:
                    logging.debug(
                        f"Getting {self.count} {self.classname()} for course {course_id} | {position[0]}/{position[1]}"
                    )
                else:
                    logging.debug(f"Getting {self.count} {self.classname()}")

                self.to_json(records)

    def _parse_statehistory(self, record, parsed):
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

    def _parse_gradehistory(self, record, parsed):
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

    def _parse_coursework(self, coursework):
        """Parse the coursework nested json into flat records for insertion
        in to database table"""
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
            self._parse_statehistory(record, parsed)
            self._parse_gradehistory(record, parsed)
            records.append(parsed)
        return records
