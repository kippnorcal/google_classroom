import itertools
import json
import logging
import os
import time
import pandas as pd
from tenacity import stop_after_attempt, wait_exponential, Retrying
from sqlalchemy.schema import DropTable
from sqlalchemy.exc import NoSuchTableError, InvalidRequestError, DataError
from timer import elapsed
import endpoints


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
        # Set to True in a subclass if the API response doesn't include course IDs.
        self.inject_course_id = False

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
        try:
            self.sql.insert_into(self.table_name, df, chunksize=10000)
        except DataError:
            # In case of failure, at least upload one-by-one to identify the bad row.
            split_dfs = [df.loc[[i]] for i in df.index]
            for df_small in split_dfs:
                try:
                    self.sql.insert_into(self.table_name, df_small)
                except DataError as error:
                    pd.set_option("display.max_columns", None)
                    logging.debug(
                        f"{self.classname()}: {error}, unable to upload {df_small}"
                    )

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

            if self.inject_course_id:
                for record in records:
                    record["courseId"] = course_id

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

    def differences_between_frames(self, df1, df2, left_on, right_on):
        """
        Merges two dataframes and splits them by which one a row comes from.

        Parameters:
            df1:        The first dataframe to be compared.
            df2:        The second dataframe to be compared.
            left_on:    The column in df1 to match the first dataframe to the second.
            right_on:   The column in df2 to match the second dataframe to the first.

        Returns:
            left_only:  A dataframe containing data only found in df1.
            right_only: A dataframe containing data only found in df2.
            both:       A dataframe containing data found in both df1 and df2.
        """
        merged = pd.merge(
            df1,
            df2,
            left_on=left_on,
            right_on=right_on,
            how="outer",
            indicator=True,
        )
        left_only = merged[merged["_merge"] == "left_only"].reset_index(drop=True)
        right_only = merged[merged["_merge"] == "right_only"].reset_index(drop=True)
        both = merged[merged["_merge"] == "both"].reset_index(drop=True)

        return (left_only, right_only, both)

    def return_cleaned_sync_data(self):
        """
        Any cleaning that must be done to provide usable data for syncing.
        Can be overwritten by subclasses in case of special logic.
        """
        return self.return_all_data().astype("str")

    def sync_data(self, data=None):
        if data is None:
            csv_name = f"{self.classname().lower()}.csv"
            data = pd.read_csv(f"sync_files/{csv_name}").astype("str")
        db_df = self.return_cleaned_sync_data()
        aliases = endpoints.CourseAliases(self.service, self.sql, self.config)
        alias_df = aliases.return_cleaned_sync_data()
        db_df = pd.merge(
            db_df, alias_df, left_on="courseId", right_on="courseId", how="inner"
        )
        data["alias"] = "d:" + data["alias"]

        (left_only, right_only, _) = self.differences_between_frames(
            data, db_df, "alias", "alias"
        )
        to_create = data[data.alias.isin(left_only.alias)].reset_index(drop=True)
        to_delete = db_df[db_df.alias.isin(right_only.alias)].reset_index(drop=True)
        return (to_create, to_delete)
