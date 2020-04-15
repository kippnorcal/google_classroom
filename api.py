from datetime import datetime, timedelta
import itertools
import json
import logging
import os
from googleapiclient.http import HttpError
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import NoSuchTableError, InvalidRequestError
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
        self.request_key = None
        self.table_name = f"GoogleClassroom_{self.classname()}"

    def request_data(self, course_id=None, date=None, next_page_token=None):
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
        logging.info(
            f"{self.classname()}: inserting {len(df)} records into {self.table_name}."
        )
        sql.insert_into(self.table_name, df)

    def _delete_local_file(self):
        """Deletes the local debug json file in /data."""
        if os.path.exists(self.filename):
            os.remove(self.filename)

    def _write_json_to_file(self, json_data):
        """Writes the json data to a debug file in /data."""
        mode = "a" if os.path.exists(self.filename) else "w"
        with open(self.filename, mode) as file:
            file.seek(0)
            json.dump(json_data, file)

    def _drop_table(self, sql):
        """
        Deletes the connected table related to this class.
        Drops rather than truncates to allow for easy schema changes without migrating.
        """
        try:
            table = sql.table(self.table_name)
            sql.engine.execute(DropTable(table))
        except NoSuchTableError as error:
            logging.debug(f"{error}: Attempted deletion, but no table exists.")

    def _generate_request_id(self, course_id, date, page):
        """
        Generates a string that can be used as a request_id for batch requesting that
        contains information on course_id, date, and page number. This is the only
        method for passing information through a batch request, and allows for
        paginating by calling the request with the same parameters again.
        NOTE: All request_ids must be unique, so the page number is necessary.
        """
        return ";".join([str(course_id), str(date), str(int(page) + 1)])

    def _generate_request_tuple(self, course_id, date, page, next_page_token):
        return (
            self.request_data(course_id, date, next_page_token),
            self._generate_request_id(course_id, date, page),
        )

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    @elapsed
    def batch_pull_data(
        self, sql, course_ids=[None], dates=[None], overwrite=True, debug=True
    ):
        """
        Executes the API request in batches based on the courses and dates, writing
        results as they come in to the DB, and returning the cumulative results.

        Parameters:
            sql:        A sqlsorcery object.
            course_ids: A list of courses that will each get a separate request.
            dates:      A list of dates that will each get a separate request.
            overwite:   If True, drops and overwrites the existing database.
            debug:      If True, also writes raw debug json results to file.

        Returns:
            all_data:   The results of the request.
        """
        if overwrite:
            self._drop_table(sql)

        if debug:
            self._delete_local_file()

        all_data = None
        remaining_requests = []

        def callback(request_id, response, exception):
            """A local callback for batch requests when they have completed."""
            course_id, date, page = request_id.split(";")

            if exception:
                logging.info(exception)
                return

            if "warnings" in response:
                # Examples of warnings include partial data availability in StudentUsage.
                for warning in response["warnings"]:
                    logging.debug(f"{warning['code']}: {warning['message']}")

            if "nextPageToken" in response:
                next_request = self._generate_request_tuple(
                    course_id, date, page, response["nextPageToken"]
                )
                remaining_requests.append(next_request)

            records = response.get(self.request_key, [])
            logging.info(
                f"{self.classname()}: received {len(records)} records from course {course_id}, date {date}, page {page}"
            )
            if len(records) > 0:
                if debug:
                    self._write_json_to_file(records)
                df = self._process_and_filter_records(records)
                self._write_to_db(sql, df)
                nonlocal all_data
                all_data = df if all_data is None else all_data.append(df)

        request_list = list(itertools.product(course_ids, dates))
        chunk_size = 1000  # The current Google batch size limit
        for i in range(0, len(request_list), chunk_size):
            request_chunk = request_list[i : i + chunk_size]
            course_ids, dates = zip(*request_chunk)
            course_ids = set(course_ids)
            dates = set(dates)
            logging.info(
                f"{self.classname()}: batch requesting data for courses {course_ids} on dates {dates}"
            )
            for (course_id, date) in request_chunk:
                request = self._generate_request_tuple(course_id, date, str(0), None)
                remaining_requests.append(request)

            while len(remaining_requests) > 0:
                batch = self.service.new_batch_http_request(callback=callback)
                for (request, request_id) in remaining_requests:
                    batch.add(request, request_id=request_id)
                remaining_requests.clear()
                batch.execute()

        return all_data


class OrgUnits(EndPoint):
    def __init__(self, service, student_org_unit=None):
        super().__init__(service)
        self.date_columns = []
        self.columns = ["name", "description", "orgUnitPath", "orgUnitId"]
        self.request_key = "organizationUnits"
        self.student_org_unit = student_org_unit

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request org unit that matches the given path"""
        return self.service.orgunits().list(customerId="my_customer")

    def filter_data(self, dataframe):
        return dataframe.loc[dataframe.name == self.student_org_unit, "orgUnitId"]


class StudentUsage(EndPoint):
    def __init__(self, service, org_unit_id):
        super().__init__(service)
        self.date_columns = ["AsOfDate", "LastUsedTime"]
        self.columns = ["Email", "AsOfDate", "LastUsedTime"]
        self.org_unit_id = org_unit_id
        self.request_key = "usageReports"

    def get_last_date(self, sql):
        """Gets the last available date of data in the database."""
        try:
            usage = pd.read_sql_table(
                self.table_name, con=sql.engine, schema=sql.schema
            )
            return usage.AsOfDate.max() if usage.AsOfDate.count() > 0 else None
        except:
            return None

    def remove_dates_after(self, sql, date):
        """Removes the given date and any after from the database."""
        logging.info(f"Deleting usage during and after {date} from {self.table_name}.")
        table = sql.table(self.table_name)
        query = table.delete().where(table.c.AsOfDate >= date)
        sql.engine.execute(query)

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all usage for the given org unit."""
        options = {
            "userKey": "all",
            "date": date,
            "pageToken": next_page_token,
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

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all guardians."""
        return (
            self.service.userProfiles()
            .guardians()
            .list(studentId="-", pageToken=next_page_token)
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

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all pending and complete guardian invites."""
        return (
            self.service.userProfiles()
            .guardianInvitations()
            .list(
                studentId="-",
                states=["PENDING", "COMPLETE"],
                pageToken=next_page_token,
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
        try:
            courses = pd.read_sql_table(
                self.table_name, con=sql.engine, schema=sql.schema
            )
            return courses.id.unique()
        except InvalidRequestError as error:
            logging.info(error)
            return None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all active courses."""
        return self.service.courses().list(
            pageToken=next_page_token, courseStates=["ACTIVE"]
        )

    def filter_data(self, dataframe):
        return dataframe[dataframe.updateTime >= self.school_year_start]


class Topics(EndPoint):
    def __init__(self, service):
        super().__init__(service)
        self.date_columns = ["updateTime"]
        self.columns = ["courseId", "topicId", "name", "updateTime"]
        self.request_key = "topic"

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all topics for this course."""
        return (
            self.service.courses()
            .topics()
            .list(pageToken=next_page_token, courseId=course_id)
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

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all teachers for this course."""
        return (
            self.service.courses()
            .teachers()
            .list(pageToken=next_page_token, courseId=course_id)
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

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all students for this course."""
        return (
            self.service.courses()
            .students()
            .list(pageToken=next_page_token, courseId=course_id)
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

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all coursework for this course."""
        return (
            self.service.courses()
            .courseWork()
            .list(pageToken=next_page_token, courseId=course_id)
        )


class StudentSubmissions(EndPoint):
    def __init__(self, service):
        super().__init__(service)
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
        ]
        self.request_key = "studentSubmissions"

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all student submissions for this course."""
        return (
            self.service.courses()
            .courseWork()
            .studentSubmissions()
            .list(pageToken=next_page_token, courseId=course_id, courseWorkId="-",)
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
