# googleclassroom

## Dependencies:
* Python3.7
* [Pipenv](https://pipenv.readthedocs.io/en/latest/)
* [Docker](https://www.docker.com/)

## Getting Started

### Setup Environment

1. Clone this repo

```
git clone https://github.com/kipp-bayarea/google_classroom.git
```

2. Create .env file with project secrets

```
# Database variables
DB_SERVER=
DB=
DB_USER=
DB_PWD=
DB_SCHEMA=

# Google API variables
STUDENT_ORG_UNIT=name of the student org unit

# Yearly settings
SCHOOL_YEAR_START=YYYY-MM-DD
```

3. Generate the `token.pickle` and `credentials.json` files.

* Use Google authentication flow.
* It can't be done through Docker.
* Either need to run it in pipenv the first time, or copy the two files from an existing working project.

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
