from datetime import datetime, timedelta
import itertools
import json
import logging
import math
import os
import time
from googleapiclient.http import HttpError
import pandas as pd
from tenacity import stop_after_attempt, wait_exponential, Retrying
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import NoSuchTableError, InvalidRequestError
from timer import elapsed


class EndPoint:
    @classmethod
    def classname(cls):
        return cls.__name__

    def __init__(self, service, sql, config):
        self.service = service
        self.sql = sql
        self.config = config
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
        df = df.astype("object")
        if self.date_columns:
            date_types = {col: "datetime64[ns]" for col in self.date_columns}
            df = df.astype(date_types)
        return df

    def _write_to_db(self, df):
        """Writes the data into the related table"""
        logging.debug(
            f"{self.classname()}: inserting {len(df)} records into {self.table_name}."
        )
        self.sql.insert_into(self.table_name, df)

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

    def _drop_table(self):
        """
        Deletes the connected table related to this class.
        Drops rather than truncates to allow for easy schema changes without migrating.
        """
        try:
            table = self.sql.table(self.table_name)
            self.sql.engine.execute(DropTable(table))
        except NoSuchTableError as error:
            logging.debug(f"{error}: Attempted deletion, but no table exists.")

    def _generate_request_id(self, course_id, date, next_page_token, page):
        """
        Generates a string that can be used as a request_id for batch requesting that
        contains information on course_id, date, and page number. This is the only
        method for passing information through a batch request, and allows for
        paginating by calling the request with the same parameters again.
        NOTE: All request_ids must be unique, so the page number is necessary.
        """
        return ";".join([str(course_id), str(date), str(next_page_token), str(page)])

    def _get_request_info(self, request_id):
        """
        Splits out request ID into its components.
        Should always reverse `_generate_request_id`
        """

        course_id, date, next_page_token, page = request_id.split(";")
        return course_id, date, next_page_token, page

    def _generate_request_tuple(self, course_id, date, next_page_token, page):
        """Generates a tuple with request data and a request ID."""
        return (
            self.request_data(course_id, date, next_page_token),
            self._generate_request_id(course_id, date, next_page_token, page),
        )

    def execute_batch_with_retry(self, batch):
        """Executes the passed in batch, with retry logi when not in debug."""
        if self.config.DEBUG:
            batch.execute()
        else:
            retryer = Retrying(
                stop=stop_after_attempt(5),
                wait=wait_exponential(multiplier=1, min=4, max=10),
            )
            retryer(batch.execute)

    @elapsed
    def batch_pull_data(self, course_ids=[None], dates=[None], overwrite=True):
        """
        Executes the API request in batches based on the courses and dates, writing
        results as they come in to the DB, and returning the cumulative results.

        Parameters:
            course_ids: A list of courses that will each get a separate request.
            dates:      A list of dates that will each get a separate request.
            overwite:   If True, drops and overwrites the existing database.

        Returns:
            all_data:   The results of the request.
        """
        if overwrite:
            self._drop_table()

        if self.config.DEBUG:
            self._delete_local_file()

        all_data = None
        quota_exceeded = False
        remaining_requests = []

        def callback(request_id, response, exception):
            """A local callback for batch requests when they have completed."""
            course_id, date, next_page_token, page = self._get_request_info(request_id)
            if next_page_token == "None":
                next_page_token = None

            if exception:
                status = exception.resp.status
                # 429: Quota exceeded.
                # Add the original request back to retry later.
                if status == 429:
                    same_request = self._generate_request_tuple(
                        course_id, date, next_page_token, page
                    )
                    remaining_requests.append(same_request)
                    nonlocal quota_exceeded
                    quota_exceeded = True
                logging.debug(exception)
                return

            if "warnings" in response:
                for warning in response["warnings"]:
                    logging.debug(f"{warning['code']}: {warning['message']}")
                    if warning["code"] == "PARTIAL_DATA_AVAILABLE":
                        for item in warning["data"]:
                            key = item["key"]
                            value = item["value"]
                            if key == "application" and value == "classroom":
                                logging.debug(f"Ignoring responses with partial data.")
                                return

            if "nextPageToken" in response:
                logging.debug(
                    f"{self.classname()}: Queueing next page from course {course_id}"
                )
                next_request = self._generate_request_tuple(
                    course_id, date, response["nextPageToken"], int(page) + 1
                )
                remaining_requests.append(next_request)

            records = response.get(self.request_key, [])
            logging.debug(
                f"{self.classname()}: received {len(records)} records from course {course_id}, date {date}, page {page}"
            )
            if len(records) > 0:
                if self.config.DEBUG:
                    self._write_json_to_file(records)
                df = self._process_and_filter_records(records)
                self._write_to_db(df)
                nonlocal all_data
                all_data = df if all_data is None else all_data.append(df)

        request_combinations = list(itertools.product(course_ids, dates))
        # Reverses because the items are taken in order from the back by popping.
        request_combinations.reverse()
        for (course_id, date) in request_combinations:
            request_tuple = self._generate_request_tuple(course_id, date, None, 0)
            remaining_requests.append(request_tuple)

        while len(remaining_requests) > 0:
            log = f"{self.classname()}: {len(remaining_requests)} requests remaining."
            if len(remaining_requests) == 1:
                _, _, _, page = self._get_request_info(remaining_requests[0][1])
                log += f" On page {page}."
            logging.info(log)
            batch = self.service.new_batch_http_request(callback=callback)
            current_batch = 0
            while len(remaining_requests) > 0 and current_batch < self.batch_size:
                current_batch += 1
                (request, request_id) = remaining_requests.pop()
                batch.add(request, request_id=request_id)
            self.execute_batch_with_retry(batch)
            if quota_exceeded:
                quota_exceeded = False
                logging.info(
                    f"{self.classname()}: Quota exceeded. Pausing for 20 seconds..."
                )
                time.sleep(20)

        return all_data


class OrgUnits(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = []
        self.columns = ["name", "description", "orgUnitPath", "orgUnitId"]
        self.request_key = "organizationUnits"
        self.batch_size = config.ORG_UNIT_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request org unit that matches the given path"""
        return self.service.orgunits().list(customerId="my_customer")

    def filter_data(self, dataframe):
        return dataframe.loc[dataframe.name == self.config.STUDENT_ORG_UNIT]


class StudentUsage(EndPoint):
    def __init__(self, service, sql, config, org_unit_id):
        super().__init__(service, sql, config)
        self.date_columns = ["AsOfDate", "LastUsedTime"]
        self.columns = ["Email", "AsOfDate", "LastUsedTime"]
        self.org_unit_id = org_unit_id
        self.request_key = "usageReports"
        self.batch_size = config.USAGE_BATCH_SIZE

    def get_last_date(self):
        """Gets the last available date of data in the database."""
        try:
            usage = pd.read_sql_table(
                self.table_name, con=self.sql.engine, schema=self.sql.schema
            )
            return usage.AsOfDate.max() if usage.AsOfDate.count() > 0 else None
        except:
            return None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all usage for the given org unit."""
        options = {
            "userKey": "all",
            "date": date,
            "pageToken": next_page_token,
            "parameters": "classroom:last_interaction_time",
        }
        if self.org_unit_id:
            # This is the CleverStudents org unit ID
            options["orgUnitID"] = self.org_unit_id
        return self.service.userUsageReport().get(**options)

    def preprocess_records(self, records):
        """Parse classroom usage data into a dataframe with one row per user."""
        new_records = []
        for record in records:
            row = {}
            row["Email"] = record.get("entity").get("userEmail")
            row["AsOfDate"] = record.get("date")
            row["LastUsedTime"] = self._parse_classroom_last_used(
                record.get("parameters")
            )
            row["ImportDate"] = datetime.today().strftime("%Y-%m-%d")
            new_records.append(row)
        return new_records

    def _parse_classroom_last_used(self, parameters):
        """Get classroom last interaction time from parameters list."""
        for parameter in parameters:
            if parameter.get("name") == "classroom:last_interaction_time":
                return parameter.get("datetimeValue")


class Guardians(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = []
        self.columns = ["studentId", "guardianId", "invitedEmailAddress"]
        self.request_key = "guardians"
        self.batch_size = config.GUARDIANS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all guardians."""
        return (
            self.service.userProfiles()
            .guardians()
            .list(
                studentId="-", pageToken=next_page_token, pageSize=self.config.PAGE_SIZE
            )
        )


class GuardianInvites(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime"]
        self.columns = [
            "studentId",
            "invitationId",
            "invitedEmailAddress",
            "state",
            "creationTime",
        ]
        self.request_key = "guardianInvitations"
        self.batch_size = config.GUARDIAN_INVITES_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all pending and complete guardian invites."""
        return (
            self.service.userProfiles()
            .guardianInvitations()
            .list(
                studentId="-",
                states=["PENDING", "COMPLETE"],
                pageToken=next_page_token,
                pageSize=self.config.PAGE_SIZE,
            )
        )


class Courses(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
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
        self.batch_size = config.COURSES_BATCH_SIZE

    def get_course_ids(self):
        try:
            courses = pd.read_sql_table(
                self.table_name, con=self.sql.engine, schema=self.sql.schema
            )
            return courses.id.unique()
        except InvalidRequestError as error:
            logging.debug(error)
            return None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all active courses."""
        return self.service.courses().list(
            pageToken=next_page_token,
            courseStates=["ACTIVE"],
            pageSize=self.config.PAGE_SIZE,
        )

    def filter_data(self, dataframe):
        return dataframe[dataframe.updateTime >= self.config.SCHOOL_YEAR_START]


class Topics(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["updateTime"]
        self.columns = ["courseId", "topicId", "name", "updateTime"]
        self.request_key = "topic"
        self.batch_size = config.TOPICS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all topics for this course."""
        return (
            self.service.courses()
            .topics()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )


class Teachers(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = [
            "courseId",
            "userId",
            "profile.name.fullName",
            "profile.emailAddress",
        ]
        self.request_key = "teachers"
        self.batch_size = config.TEACHERS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all teachers for this course."""
        return (
            self.service.courses()
            .teachers()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )


class Students(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = [
            "courseId",
            "userId",
            "profile.name.fullName",
            "profile.emailAddress",
        ]
        self.request_key = "students"
        self.batch_size = config.STUDENTS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all students for this course."""
        return (
            self.service.courses()
            .students()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )


class CourseWork(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime", "updateTime", "dueDate"]
        self.columns = [
            "courseId",
            "id",
            "title",
            "description",
            "state",
            "alternateLink",
            "creationTime",
            "updateTime",
            "dueDate",
            "maxPoints",
            "workType",
            "assigneeMode",
            "submissionModificationMode",
            "creatorUserId",
            "topicId",
        ]
        self.request_key = "courseWork"
        self.batch_size = config.COURSEWORK_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all coursework for this course."""
        return (
            self.service.courses()
            .courseWork()
            .list(
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
                pageToken=next_page_token,
            )
        )

    def preprocess_records(self, records):
        """Parse classroom usage data into a dataframe with one row per user."""
        for record in records:
            if "dueDate" in record:
                year = record["dueDate"]["year"]
                month = record["dueDate"]["month"]
                day = record["dueDate"]["day"]
                if "dueTime" in record:
                    hours = record["dueTime"].get("hours", 0)
                    minutes = record["dueTime"].get("minutes", 0)
                    record["dueDate"] = datetime(year, month, day, hours, minutes)
                else:
                    record["dueDate"] = datetime(year, month, day)
        return records


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
        ]
        self.request_key = "studentSubmissions"
        self.batch_size = config.SUBMISSIONS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all student submissions for this course."""
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

    def preprocess_records(self, records):
        """Parse the coursework nested json into flat records for insertion
        in to database table"""
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
            }
            self._parse_statehistory(record, parsed)
            self._parse_gradehistory(record, parsed)
            new_records.append(parsed)
        return new_records


class CourseAliases(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["courseId", "alias"]
        self.request_key = "aliases"
        self.batch_size = config.ALIASES_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all aliases for this course."""
        return (
            self.service.courses()
            .aliases()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )


class Invitations(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["id", "userId", "courseId", "role"]
        self.request_key = "invitations"
        self.batch_size = config.INVITATIONS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request all invitations for this course."""
        return self.service.invitations().list(
            pageToken=next_page_token,
            courseId=course_id,
            pageSize=self.config.PAGE_SIZE,
        )
