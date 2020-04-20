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
- Pipenv can be installed via Pip or Homebrew, and is only needed for local development or to generate an initial token.

3. Create .env file with project secrets

Database variables are configured in the format used by [Sqlsorcery](https://sqlsorcery.readthedocs.io/en/latest/cookbook/environment.html).

STUDENT_ORG_UNIT represents the Google Admin organizational unit that students belong to.
It will filter student reports to that specific organization.

```
# Database variables
DB_TYPE=The type of database you are using. Current options: mssql, postgres, sqlite
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# OPTIONAL: Data Pulls To Enable. Set to "YES" to include that pull.
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
PAGE_SIZE=The number of items to page at once.

# Email notification variables
# OPTIONAL: Set DISABLE_MAILER to "YES" if you do not want email notifications to be sent.
DISABLE_MAILER=
SENDER_EMAIL=
SENDER_PWD=
RECIPIENT_EMAIL=
# If using a standard Gmail account you can set these to smtp.gmail.com on port 465
EMAIL_SERVER=
EMAIL_PORT=

# Google API variables
STUDENT_ORG_UNIT=name of the student org unit

# Yearly settings
SCHOOL_YEAR_START=YYYY-MM-DD
```

4. Enable APIs in Developer Console

- Navigate to the [API library](https://console.developers.google.com/apis/library) in the developer console.
- Search for Google Classroom, and Enable it.
- Search for Admin SDK, and Enable it.

5. Generate the `token.pickle` and `credentials.json` files.

The token and credentials files authenticate the Google user in order to run this application.

Credentials.json:

- Follow instructions to [create authorization credentials](https://developers.google.com/identity/protocols/oauth2/web-server#creatingcred)
  - For local development: Authorized redirect URIs should be `http://localhost/`.
- Existing credentials can be accessed through the [Google Developer Console](https://console.developers.google.com/apis/credentials?pli=1).

Token.pickle: The initial token.pickle file must be generated locally prior to using Docker.
This can be done by running the script locally the first time.

```
pipenv install --skip-lock
pipenv run python main.py
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

Optional flags will include different types of pulls (can also be done via env variables):

- `--usage`
- `--courses`
- `--topics`
- `--coursework`
- `--students`
- `--teachers`
- `--guardians`
- `--submissions`
- `--invites`

Use the flag `--debug` to turn on debug logging.

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
