# Configuration file for types of configurations.

import os
import argparse
from sqlsorcery import MSSQL, PostgreSQL, SQLite


def get_args():
    parser = argparse.ArgumentParser(description="Pick which ones")
    parser.add_argument("--all", help="Import all data", action="store_true")
    parser.add_argument(
        "--usage", help="Import student usage data", action="store_true"
    )
    parser.add_argument("--courses", help="Import course lists", action="store_true")
    parser.add_argument("--topics", help="Import course topics", action="store_true")
    parser.add_argument("--aliases", help="Import course aliases", action="store_true")
    parser.add_argument(
        "--coursework", help="Import course assignments", action="store_true"
    )
    parser.add_argument(
        "--students", help="Import student rosters", action="store_true"
    )
    parser.add_argument(
        "--teachers", help="Import teacher rosters", action="store_true"
    )
    parser.add_argument(
        "--guardians", help="Import student guardians", action="store_true"
    )
    parser.add_argument(
        "--invitations", help="Import course invitations", action="store_true"
    )
    parser.add_argument(
        "--submissions",
        help="Import student coursework submissions",
        action="store_true",
    )
    parser.add_argument(
        "--invites", help="Import guardian invite statuses", action="store_true"
    )
    parser.add_argument(
        "--announcements", help="Import course announcements", action="store_true"
    )
    parser.add_argument("--meet", help="Import Meet data", action="store_true")
    parser.add_argument(
        "--debug", help="Set logging level for troubleshooting", action="store_true"
    )
    parser.add_argument(
        "--debugfile", help="Log raw json to a file", action="store_true"
    )
    args, _ = parser.parse_known_args()
    return args


class Config(object):
    """Base configuration object"""

    args = get_args()

    # General config
    ACCOUNT_EMAIL = os.getenv("ACCOUNT_EMAIL")
    STUDENT_ORG_UNIT = os.getenv("STUDENT_ORG_UNIT")
    SCHOOL_YEAR_START = os.getenv("SCHOOL_YEAR_START")

    # DB config
    DB_TYPE = os.getenv("DB_TYPE")
    DB_SERVER = os.getenv("DB_SERVER")
    DB = os.getenv("DB")
    DB_USER = os.getenv("DB_USER")
    DB_PWD = os.getenv("DB_PWD")
    DB_SCHEMA = os.getenv("DB_SCHEMA")

    # Debug config
    DEBUG = args.debug
    DEBUGFILE = args.debugfile

    # Which endpoints to pull data from
    PULL_USAGE = os.getenv("PULL_USAGE") == "YES" or args.usage or args.all
    PULL_COURSES = os.getenv("PULL_COURSES") == "YES" or args.courses or args.all
    PULL_TOPICS = os.getenv("PULL_TOPICS") == "YES" or args.topics or args.all
    PULL_COURSEWORK = (
        os.getenv("PULL_COURSEWORK") == "YES" or args.coursework or args.all
    )
    PULL_ALIASES = os.getenv("PULL_ALIASES") == "YES" or args.aliases or args.all
    PULL_STUDENTS = os.getenv("PULL_STUDENTS") == "YES" or args.students or args.all
    PULL_TEACHERS = os.getenv("PULL_TEACHERS") == "YES" or args.teachers or args.all
    PULL_GUARDIANS = os.getenv("PULL_GUARDIANS") == "YES" or args.guardians or args.all
    PULL_SUBMISSIONS = (
        os.getenv("PULL_SUBMISSIONS") == "YES" or args.submissions or args.all
    )
    PULL_INVITATIONS = (
        os.getenv("PULL_INVITATIONS") == "YES" or args.invitations or args.all
    )
    PULL_GUARDIAN_INVITES = (
        os.getenv("PULL_GUARDIAN_INVITES") == "YES" or args.invites or args.all
    )
    PULL_ANNOUNCEMENTS = (
        os.getenv("PULL_ANNOUNCEMENTS") == "YES" or args.announcements or args.all
    )
    PULL_MEET = os.getenv("PULL_MEET") == "YES" or args.aliases or args.all

    # Email configuration
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    SENDER_PWD = os.getenv("SENDER_PWD")
    EMAIL_SERVER = os.getenv("EMAIL_SERVER")
    EMAIL_PORT = os.getenv("EMAIL_PORT")
    RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")
    DISABLE_MAILER = os.getenv("DISABLE_MAILER") == "YES"

    # Endpoint configuration
    ORG_UNIT_BATCH_SIZE = int(os.getenv("ORG_UNIT_BATCH_SIZE") or 1000)
    USAGE_BATCH_SIZE = int(os.getenv("USAGE_BATCH_SIZE") or 1000)
    COURSES_BATCH_SIZE = int(os.getenv("COURSES_BATCH_SIZE") or 1000)
    TOPICS_BATCH_SIZE = int(os.getenv("TOPICS_BATCH_SIZE") or 1000)
    COURSEWORK_BATCH_SIZE = int(os.getenv("COURSEWORK_BATCH_SIZE") or 120)
    STUDENTS_BATCH_SIZE = int(os.getenv("STUDENTS_BATCH_SIZE") or 1000)
    TEACHERS_BATCH_SIZE = int(os.getenv("TEACHERS_BATCH_SIZE") or 1000)
    GUARDIANS_BATCH_SIZE = int(os.getenv("GUARDIANS_BATCH_SIZE") or 1000)
    SUBMISSIONS_BATCH_SIZE = int(os.getenv("SUBMISSIONS_BATCH_SIZE") or 120)
    GUARDIAN_INVITES_BATCH_SIZE = int(os.getenv("GUARDIAN_INVITES_BATCH_SIZE") or 1000)
    ALIASES_BATCH_SIZE = int(os.getenv("ALIASES_BATCH_SIZE") or 1000)
    INVITATIONS_BATCH_SIZE = int(os.getenv("INVITATIONS_BATCH_SIZE") or 1000)
    ANNOUNCEMENTS_BATCH_SIZE = int(os.getenv("ANNOUNCEMENTS_BATCH_SIZE") or 1000)
    MEET_BATCH_SIZE = int(os.getenv("MEET_BATCH_SIZE") or 1000)
    PAGE_SIZE = int(os.getenv("PAGE_SIZE") or 1000)


class TestConfig(Config):
    DB_TYPE = "sqlite"
    DEBUG = True
    DEBUGFILE = False
    PULL_USAGE = True
    PULL_COURSES = True
    PULL_TOPICS = True
    PULL_COURSEWORK = True
    PULL_STUDENTS = True
    PULL_TEACHERS = True
    PULL_GUARDIANS = True
    PULL_SUBMISSIONS = True
    PULL_GUARDIAN_INVITES = True
    PULL_INVITATIONS = True
    PULL_GUARDIAN_INVITES = True
    PULL_ANNOUNCEMENTS = True
    DISABLE_MAILER = True
    SCHOOL_YEAR_START = "2020-01-01"
    DB_TYPE = "sqlite"
    DB_SERVER = None
    DB = "tests.db"
    DB_USER = None
    DB_PWD = None
    DB_SCHEMA = None
    STUDENT_ORG_UNIT = "Test Organization 2"
    ORG_UNIT_BATCH_SIZE = 1000
    USAGE_BATCH_SIZE = 1000
    COURSES_BATCH_SIZE = 1000
    TOPICS_BATCH_SIZE = 1000
    COURSEWORK_BATCH_SIZE = 120
    STUDENTS_BATCH_SIZE = 1000
    TEACHERS_BATCH_SIZE = 1000
    GUARDIANS_BATCH_SIZE = 1000
    SUBMISSIONS_BATCH_SIZE = 120
    GUARDIAN_INVITES_BATCH_SIZE = 1000
    ALIASES_BATCH_SIZE = 1000
    INVITATIONS_BATCH_SIZE = 1000
    ANNOUNCEMENTS_BATCH_SIZE = 1000
    PAGE_SIZE = 1000


def db_generator(config):
    db_type = config.DB_TYPE
    default_config = {
        "schema": config.DB_SCHEMA,
        "server": config.DB_SERVER,
        "port": None,
        "db": config.DB,
        "user": config.DB_USER,
        "pwd": config.DB_PWD,
    }
    if db_type == "mssql":
        return MSSQL(**default_config)
    elif db_type == "postgres":
        return PostgreSQL(**default_config)
    elif db_type == "sqlite":
        return SQLite(path=config.DB)
    else:
        raise Exception()
