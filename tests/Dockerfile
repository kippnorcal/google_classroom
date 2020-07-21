FROM python:3.7
WORKDIR /code
# SQLSorcery Dependencies
RUN wget https://packages.microsoft.com/debian/9/prod/pool/main/m/msodbcsql17/msodbcsql17_17.5.2.1-1_amd64.deb
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y unixodbc unixodbc-dev
RUN yes | dpkg -i msodbcsql17_17.5.2.1-1_amd64.deb
# Python Dependencies   
RUN pip install pipenv
COPY Pipfile .
RUN pipenv install --skip-lock
COPY google_classroom/ .
COPY tests/ tests/
COPY wait-for-it.sh wait-for-it.sh
ENTRYPOINT [ "./wait-for-it.sh", "database:1433", "--", "pipenv", "run", "python", "-m", "pytest", "-s", "-v"]
