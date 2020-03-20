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
