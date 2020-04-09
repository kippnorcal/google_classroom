from datetime import datetime, timedelta
import json
import logging
import os
from googleapiclient.http import HttpError
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.schema import DropTable
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
        self.request_key = None
        self.table_name = f"GoogleClassroom_{self.classname()}"

    def request_data(self):
        """
        Returns a request object for calling the Google Classroom API for that class.
        Must be overridden by a subclass.
        """
        raise Exception("Request function must be overridden in subclass.")

    def preprocess_records(self, records):
        """
        Any preprocessing that needs to be done to records before they are mapped to columns.
        Intended to be overridden by subclasses as needed.
        """
        return records

    def filter_data(self, dataframe):
        """
        Any basic filtering done after data is converted into a dataframe.
        Intended to be overridden by subclasses as needed.
        """
        return dataframe

    def _process_and_filter_records(self, records):
        """Processes incoming records and converts them into a cleaned dataframe"""
        new_records = self.preprocess_records(records)
        df = pd.json_normalize(new_records)
        df = df.reindex(columns=self.columns)
        df = self.filter_data(df)
        if self.date_columns:
            date_types = {col: "datetime64[ns]" for col in self.date_columns}
            df = df.astype(date_types)
        return df

    def _write_to_db(self, sql, df):
        """Writes the data into the related table"""
        logging.info(f"{self.classname()}: inserting {len(df)} records into {self.table_name}.")
        sql.insert_into(self.table_name, df)

    def _delete_local_file(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def _write_json_to_file(self, json_data):
        mode = "a" if os.path.exists(self.filename) else "w"
        with open(self.filename, mode) as file:
            file.seek(0)
            json.dump(json_data, file)

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @elapsed
    def get_and_write_to_db(self, sql, course_ids=[None], overwrite=True, debug=True):
        """Execute API request and write results to json file."""
        # In cases where everything is overwritten, drop the table first.
        # Dropping rather than truncating allows for easy schema changes without migrating.
        if overwrite:
            try:
                # Try/catch in case table doesn't exist.
                table = sql.table(self.table_name)
                sql.engine.execute(DropTable(table))
            except:
                pass

        if debug:
            self._delete_local_file()

        # Keep track of data cumulatively so that it can be returned at the end.
        all_data = pd.DataFrame()

        # For some endpoints, each course must be requested separately.
        # TODO: Batch course requests together to speed up processing.
        for idx, course_id in enumerate(course_ids):
            logging.info(f"{self.classname()}: processing course {idx + 1}/{len(course_ids)}")
            self.course_id = course_id
            self.next_page_token = ""
            count = 1
            while self.next_page_token is not None:
                try:
                    results = self.request_data().execute()
                except HttpError as error:
                    # Currently this can happen with StudentUsage when the date is too recent.
                    logging.debug(error)
                    return pd.DataFrame()

                if "warnings" in results:
                    # Examples of warnings include partial data availability in StudentUsage.
                    for warning in results["warnings"]:
                        logging.debug(f"{warning['code']}: {warning['message']}")

                records = results.get(self.request_key, [])
                logging.info(
                    f"{self.classname()}: retrieved {len(records)} records from page {count}")
                count += 1
                self.next_page_token = results.get("nextPageToken", None)

                if len(records) > 0:
                    if debug:
                        # Log results to a text file for audit purposes
                        self._write_json_to_file(records)

                    #  Process the records and write them to a database.
                    df = self._process_and_filter_records(records)
                    self._write_to_db(sql, df)
                    all_data = all_data.append(df) if not all_data.empty else df
        return all_data


class OrgUnits(EndPoint):
    def __init__(self, service, student_org_unit=None):
        super().__init__(service)
        self.date_columns = []
        self.columns = ["name", "description", "orgUnitPath", "orgUnitId"]
        self.request_key = "organizationUnits"
        self.student_org_unit = student_org_unit

    def request_data(self):
        """Request org unit that matches the given path"""
        return self.service.orgunits().list(customerId="my_customer")

    def filter_data(self, dataframe):
        return dataframe.loc[dataframe.name == self.student_org_unit, "orgUnitId"]


class StudentUsage(EndPoint):
    def __init__(self, service, org_unit_id):
        super().__init__(service)
        self.date_columns = ["AsOfDate", "LastUsedTime"]
        self.columns = ["Email", "AsOfDate", "LastUsedTime"]
        self.two_days_ago = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        self.org_unit_id = org_unit_id
        self.request_key = "usageReports"

    def request_data(self):
        """Request all usage for the given org unit."""
        # TODO: This should use the last date already in the database and request all data from that
        #       date onwards. This is because partial data can come through on a given day, and
        #       there is no indication that a day was incomplete. This allows for idempotence while
        #       not dropping tables like the other classes.
        options = {
            "userKey": "all",
            "date": self.two_days_ago,
            "pageToken": self.next_page_token,
        }
        if self.org_unit_id:
            # This is the CleverStudents org unit ID
            options["orgUnitID"] = self.org_unit_id
        return self.service.userUsageReport().get(**options)

    def preprocess_records(self, usage_data):
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
        self.request_key = "guardians"

    def request_data(self):
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

    def request_data(self):
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
    def __init__(self, service, school_year_start):
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
        self.request_key = "courses"
        self.school_year_start = school_year_start

    def get_course_ids(self, sql):
        courses = pd.read_sql_table(self.table_name, con=sql.engine, schema=sql.schema)
        return courses.id.unique()

    def request_data(self):
        """Request all active courses."""
        return self.service.courses().list(
            pageToken=self.next_page_token, courseStates=["ACTIVE"]
        )

    def filter_data(self, dataframe):
        return dataframe[dataframe.updateTime >= self.school_year_start]


class Topics(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["updateTime"]
        self.columns = ["courseId", "topicId", "name", "updateTime"]
        self.request_key = "topic"

    def request_data(self):
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
        self.request_key = "teachers"

    def request_data(self):
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
        self.request_key = "students"

    def request_data(self):
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
        self.request_key = "courseWork"

    def request_data(self):
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
        self.request_key = "studentSubmissions"

    def request_data(self):
        """Request all student submissions for this course."""
        return (
            self.service.courses()
            .courseWork()
            .studentSubmissions()
            .list(courseId=self.course_id, courseWorkId="-")
        )

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

    def preprocess_records(self, coursework):
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
