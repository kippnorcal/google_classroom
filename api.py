from datetime import datetime
import itertools
import json
import logging
import os
import time
import pandas as pd
from tenacity import stop_after_attempt, wait_exponential, Retrying
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import NoSuchTableError, InvalidRequestError
from timer import elapsed


class EndPoint:
    """
    A generic endpoint, intended to be overwritten by a subclass to implement details
    around what URLs to call and how to process incoming data.

    Parameters:
        service:    A Google API service to use for the request.
        sql:        A Sqlsorcery object to read and write from the DB.
        config:     A config object for customizing the request.

    Returns:
        An instance of the endpoint that can be called to make a request.
    """

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

    def return_all_data(self):
        """Returns all the data in the associated table"""
        try:
            return pd.read_sql_table(
                self.table_name, con=self.sql.engine, schema=self.sql.schema
            )
        except InvalidRequestError as error:
            logging.debug(error)
            return None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """
        Returns a request object for calling the Google Classroom API for that class.
        Must be overridden by a subclass.
        """
        raise Exception("Request function must be overridden in subclass.")

    def preprocess_records(self, records):
        """
        Any preprocessing that needs to be done to records before they are mapped to
        columns. Intended to be overridden by subclasses as needed.
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
        logging.debug(f"{self.classname()}: processing {len(records)} records.")
        new_records = self.preprocess_records(records)
        df = pd.json_normalize(new_records)
        df = df.reindex(columns=self.columns)
        df = self.filter_data(df)
        df = df.astype("object")
        df = self._convert_dates(df)
        return df

    def _convert_dates(self, df):
        """Convert date columns to the actual date time."""
        if self.date_columns:
            date_types = {col: "datetime64[ns]" for col in self.date_columns}
            df = df.astype(date_types)
        return df

    def _write_to_db(self, df):
        """Writes the data into the related table"""
        logging.debug(
            f"{self.classname()}: inserting {len(df)} records into {self.table_name}."
        )
        self.sql.insert_into(self.table_name, df, chunksize=10000)

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

        values = request_id.split(";")
        cleaned_values = [None if val == "None" else val for val in values]
        course_id, date, next_page_token, page = cleaned_values

        return course_id, date, next_page_token, page

    def _generate_request_tuple(self, course_id, date, next_page_token, page):
        """Generates a tuple with request data and a request ID."""
        return (
            self.request_data(course_id, date, next_page_token),
            self._generate_request_id(course_id, date, next_page_token, page),
        )

    def _execute_batch_with_retry(self, batch):
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
        """
        if overwrite:
            self._drop_table()

        if self.config.DEBUGFILE:
            self._delete_local_file()

        batch_data = []
        quota_exceeded = False
        remaining_requests = []

        def callback(request_id, response, exception):
            """A local callback for batch requests when they have completed."""
            course_id, date, next_page_token, page = self._get_request_info(request_id)

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
                                logging.debug("Ignoring responses with partial data.")
                                return

            if "nextPageToken" in response:
                logging_string = f"{self.classname()}: Queueing next page"
                logging_string += f" from course {course_id}." if course_id else "."
                logging.debug(logging_string)
                next_request = self._generate_request_tuple(
                    course_id, date, response["nextPageToken"], int(page) + 1
                )
                remaining_requests.append(next_request)

            records = response.get(self.request_key, [])
            logging_string = f"{self.classname()}: received {len(records)} records"
            logging_string += f", course {course_id}" if course_id else ""
            logging_string += f", date {date}" if date else ""
            logging_string += f", page {page}" if page else ""
            logging_string += "."
            logging.debug(logging_string)
            nonlocal batch_data
            batch_data.extend(records)

        logging.info(f"{self.classname()}: Generating requests...")
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

            # Load up a new batch with requests from remaining requests
            batch = self.service.new_batch_http_request(callback=callback)
            current_batch = 0
            while len(remaining_requests) > 0 and current_batch < self.batch_size:
                current_batch += 1
                (request, request_id) = remaining_requests.pop()
                batch.add(request, request_id=request_id)
            self._execute_batch_with_retry(batch)

            # Process the results of the batch.
            if len(batch_data) > 0:
                if self.config.DEBUGFILE:
                    self._write_json_to_file(batch_data)
                df = self._process_and_filter_records(batch_data)
                self._write_to_db(df)
                batch_data = []

            # Pause if quota exceeded. 20s because the quota is a sliding time window.
            if quota_exceeded:
                quota_exceeded = False
                logging.info(
                    f"{self.classname()}: Quota exceeded. Pausing for 20 seconds..."
                )
                time.sleep(20)


class OrgUnits(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
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
        self.date_columns = ["AsOfDate", "LastUsedTime", "ImportDate"]
        self.columns = ["Email", "AsOfDate", "LastUsedTime", "ImportDate"]
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
        except ValueError:
            # Table doesn't yet exist.
            return None

    def request_data(self, course_id=None, date=None, next_page_token=None):
        options = {
            "userKey": "all",
            "date": date,
            "pageToken": next_page_token,
            "parameters": "classroom:last_interaction_time",
        }
        if self.org_unit_id:
            options["orgUnitID"] = self.org_unit_id
        return self.service.userUsageReport().get(**options)

    def preprocess_records(self, records):
        new_records = []
        for record in records:
            row = {}
            row["Email"] = record.get("entity").get("userEmail")
            row["AsOfDate"] = record.get("date")
            row["LastUsedTime"] = self._get_date_from_params(record.get("parameters"))
            row["ImportDate"] = datetime.today().strftime("%Y-%m-%d")
            new_records.append(row)
        return new_records

    def _get_date_from_params(self, parameters):
        for parameter in parameters:
            return parameter.get("datetimeValue")


class Guardians(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["studentId", "guardianId", "invitedEmailAddress"]
        self.request_key = "guardians"
        self.batch_size = config.GUARDIANS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
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

    def request_data(self, course_id=None, date=None, next_page_token=None):
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
            "fullName",
            "emailAddress",
        ]
        self.request_key = "teachers"
        self.batch_size = config.TEACHERS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .teachers()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )

    def preprocess_records(self, records):
        for record in records:
            record["fullName"] = record.get("profile").get("name").get("fullName")
            record["emailAddress"] = record.get("profile").get("emailAddress")
        return records


class Students(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["courseId", "userId", "fullName", "emailAddress"]
        self.request_key = "students"
        self.batch_size = config.STUDENTS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .students()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )

    def preprocess_records(self, records):
        for record in records:
            record["fullName"] = record.get("profile").get("name").get("fullName")
            record["emailAddress"] = record.get("profile").get("emailAddress")
        return records


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
            }
            self._parse_state_history(record, parsed)
            self._parse_grade_history(record, parsed)
            new_records.append(parsed)
        return new_records


class CourseAliases(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.columns = ["courseId", "alias"]
        self.request_key = "aliases"
        self.batch_size = config.ALIASES_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
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
        return self.service.invitations().list(
            pageToken=next_page_token,
            courseId=course_id,
            pageSize=self.config.PAGE_SIZE,
        )


class Announcements(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = ["creationTime", "updateTime", "scheduledTime"]
        self.columns = [
            "id",
            "courseId",
            "text",
            "state",
            "alternateLink",
            "creationTime",
            "updateTime",
            "scheduledTime",
            "assigneeMode",
            "creatorUserId",
        ]
        self.request_key = "announcements"
        self.batch_size = config.ANNOUNCEMENTS_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        return (
            self.service.courses()
            .announcements()
            .list(
                pageToken=next_page_token,
                courseId=course_id,
                pageSize=self.config.PAGE_SIZE,
            )
        )


class Meet(EndPoint):
    def __init__(self, service, sql, config):
        super().__init__(service, sql, config)
        self.date_columns = []
        self.columns = [
            "conference_id",
            "device_type",
            "display_name",
            "duration_seconds",
            "endpoint_id",
            "identifier",
            "identifier_type",
            "ip_address",
            "is_external",
            "meeting_code",
            "organizer_email",
            "item_time",
            "event_name",
        ]
        self.request_key = "items"
        self.batch_size = config.MEET_BATCH_SIZE

    def request_data(self, course_id=None, date=None, next_page_token=None):
        """Request Google Meet events (currently only call_ended)"""
        options = {
            "applicationName": "meet",
            "userKey": "all",
            "eventName": "call_ended",
            "pageToken": next_page_token,
        }
        return self.service.activities().list(**options)

    def preprocess_records(self, records):
        """Pull out parameter data from the returned Google Meet call event"""
        new_records = []
        for record in records:
            event_records = record.get("events")
            item_time = record.get("id").get("time")
            for event_record in event_records:
                event_name = event_record.get("name")
                if event_name == "call_ended":
                    new_record = {"item_time": item_time, "event_name": event_name}
                    for subrecord in event_record.get("parameters"):
                        name = subrecord.get("name")
                        value = (
                            subrecord.get("value")
                            or subrecord.get("intValue")
                            or subrecord.get("boolValue")
                        )
                        new_record[name] = value
                    new_records.append(new_record)
        return new_records
