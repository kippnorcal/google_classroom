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

- Pipenv is most commonly installed via Homebrew.
- Docker can be installed directly from the website at docker.com.

```
brew install pipenv
```

2. Create .env file with project secrets

Database variables are configured in the format used by [Sqlsorcery](https://github.com/dchess/sqlsorcery).

(optional) STUDENT_ORG_UNIT represents the Google Admin organizational unit that students belong to.
It will filter student reports to that specific organization.

```
# Database variables
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# Google API variables
STUDENT_ORG_UNIT=alphanumeric string id for the org unit (retrieve through Admin API)

# Yearly settings
SCHOOL_YEAR_START=YYYY-MM-DD
```

3. Create a folder for logging and caching data.

```
mkdir data
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

### Yearly maintenance

1. Confirm the org unit ID (used to get Student Usage) in the .env.
