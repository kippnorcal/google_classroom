# googleclassroom

## Dependencies:

- Python3.7
- [Pipenv](https://pipenv.readthedocs.io/en/latest/)
- [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
git clone https://github.com/kipp-bayarea/google_classroom.git
```

2. Install dependencies

- Docker can be installed directly from the website at docker.com.

3. Create .env file with project secrets

```
# Basic Configuration Info
ACCOUNT_EMAIL=Email of admin account that will be used to pull data.
STUDENT_ORG_UNIT=Name of the Google Admin organizational unit for students (optional — filters student reports to that organization)
SCHOOL_YEAR_START=YYYY-MM-DD

# Database variables
DB_TYPE=The type of database you are using. Current options: mssql, postgres, sqlite
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# (Optional) Data Pulls To Enable. Set to "YES" to include that pull.
# These can be left out in favor of command line arguments.
PULL_USAGE=
PULL_COURSES=
PULL_TOPICS=
PULL_COURSEWORK=
PULL_STUDENTS=
PULL_TEACHERS=
PULL_GUARDIANS=
PULL_SUBMISSIONS=
PULL_GUARDIAN_INVITES=
PULL_ALIASES=
PULL_INVITATIONS=
PULL_ANNOUNCEMENTS=
PULL_MEET=

# (Optional) Batch parameters. Can be configured and changed to optimize performance.
# *_BATCH_SIZE is the number of dates or courses to batch at a time. MAX: 1000
# Lower batch sizes are useful for high volume or slow endpoints to avoid timeouts.
ORG_UNIT_BATCH_SIZE=
USAGE_BATCH_SIZE=
COURSES_BATCH_SIZE=
TOPICS_BATCH_SIZE=
COURSEWORK_BATCH_SIZE=
STUDENTS_BATCH_SIZE=
TEACHERS_BATCH_SIZE=
GUARDIANS_BATCH_SIZE=
SUBMISSIONS_BATCH_SIZE=
GUARDIAN_INVITES_BATCH_SIZE=
ALIASES_BATCH_SIZE=
INVITATIONS_BATCH_SIZE=
ANNOUNCEMENTS_BATCH_SIZE=
MEET_BATCH_SIZE=
PAGE_SIZE=The number of items to page at once.

# Email notification variables
# Set DISABLE_MAILER to "YES" if you do not want email notifications to be sent.
DISABLE_MAILER=
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=
# If using a standard Gmail account you can set these to smtp.gmail.com on port 465
EMAIL_SERVER=
EMAIL_PORT=
```

4. Enable APIs in Developer Console

- Navigate to the [API library](https://console.developers.google.com/apis/library) in the developer console.
- Search for Google Classroom, and Enable it.
- Search for Admin SDK, and Enable it.

5. Create a service account.

- In the Google Developer Console (console.developers.google.com), go to Credentials.
- Click on "Create Credentials -> Service Account"
- Create a name for your service account.
- Select the "Owner" role for the service account.
- Create a key, saving the result file as `service.json`.
- Check the box for "Enable G Suite Domain-Wide Delegation"
- Click "Done".

6. Add scopes for the service account.

- In the Google Admin Console (admin.google.com), go to Security.
- Click on "Advanced Settings -> Manage API client access"
- For the client name, use the Unique ID of the service account.
- In the API Scopes, add the following scopes and click "Authorize".

```
https://www.googleapis.com/auth/admin.directory.orgunit,
https://www.googleapis.com/auth/admin.reports.usage.readonly,
https://www.googleapis.com/auth/classroom.announcements,
https://www.googleapis.com/auth/classroom.courses,
https://www.googleapis.com/auth/classroom.coursework.students,
https://www.googleapis.com/auth/classroom.guardianlinks.students,
https://www.googleapis.com/auth/classroom.profile.emails,
https://www.googleapis.com/auth/classroom.rosters,
https://www.googleapis.com/auth/classroom.student-submissions.students.readonly,
https://www.googleapis.com/auth/classroom.topics,
https://www.googleapis.com/auth/admin.reports.audit.readonly
```

### Running the job

Build the Docker image

```
docker build -t google_classroom .
```

Run the job

```
docker run --rm -it google_classroom
```

Run the job using a database on localhost

```
docker run --rm -it --network host google_classroom
```

Run the job locally

```
pipenv install --skip-lock (first time)
pipenv run python main.py
```

Optional flags will include different types of pulls (can also be done via env variables):

- `--all` (for pulling all data)
- `--usage`
- `--courses`
- `--topics`
- `--coursework`
- `--students`
- `--teachers`
- `--guardians`
- `--submissions`
- `--invites`
- `--aliases`
- `--invitations`
- `--announcements`
- `--meet`

Use the flag `--debug` to turn on debug logging.
Use the flag `--debugfile` to save raw json to a file for backup / auditing.

### Running Tests

Tests are located in tests,py, and can be run with either of the following commands:

Locally:

```
pipenv run py.test -s -v tests.py
```

On Docker:

```
docker build -t google_classroom .
docker run --rm -it google_classroom --test
```

When making changes, please run tests to make sure you have not broken anything.

### Yearly maintenance

1. Confirm the org unit ID (used to get Student Usage) in the .env.
