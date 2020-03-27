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
        key = self.classname()
        return f"{key[0].lower()}{key[1:]}"

    def to_json(self, records):
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
        with open(self.filename) as f:
            data = json.load(f)
        df = pd.json_normalize(data)
        df = df[self.columns]
        if self.date_columns:
            date_types = {col: "datetime64[ns]" for col in self.date_columns}
            df = df.astype(date_types)
        return df

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get(self, course_id=None, position=None):
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
        return self.service.userUsageReport().get(
            userKey="all",
            date=self.two_days_ago,
            orgUnitID=f"id:{self.org_unit_id}",
            pageToken=self.next_page_token,
        )

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def get(self, position=None):
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
        return (
            self.service.courses()
            .courseWork()
            .list(pageToken=self.next_page_token, courseId=self.course_id)
        )
