# Configuration file for types of configurations.

import os
import argparse
from sqlsorcery import MSSQL, PostgreSQL, SQLite


def get_args():
    parser = argparse.ArgumentParser(description="Pick which ones")
    parser.add_argument("--usage", help="Import student usage data", action="store_true")
    parser.add_argument("--courses", help="Import course lists", action="store_true")
    parser.add_argument("--topics", help="Import course topics", action="store_true")
    parser.add_argument("--coursework", help="Import course assignments", action="store_true")
    parser.add_argument("--students", help="Import student rosters", action="store_true")
    parser.add_argument("--teachers", help="Import teacher rosters", action="store_true")
    parser.add_argument("--guardians", help="Import student guardians", action="store_true")
    parser.add_argument(
        "--submissions", help="Import student coursework submissions", action="store_true"
    )
    parser.add_argument("--invites", help="Import guardian invite statuses", action="store_true")
    parser.add_argument(
        "--debug", help="Set logging level for troubleshooting", action="store_true"
    )
    return parser.parse_args()


class Config(object):
    """Base configuration object"""
    args = get_args()
    STUDENT_ORG_UNIT = os.getenv("STUDENT_ORG_UNIT")
    SCHOOL_YEAR_START = os.getenv("SCHOOL_YEAR_START")
    DB_TYPE = os.getenv("DB_TYPE")
    DEBUG = args.debug
    PULL_USAGE = os.getenv("PULL_USAGE") == "YES" or args.usage
    PULL_COURSES = os.getenv("PULL_COURSES") == "YES" or args.courses
    PULL_TOPICS = os.getenv("PULL_TOPICS") == "YES" or args.topics
    PULL_COURSEWORK = os.getenv("PULL_COURSEWORK") == "YES" or args.coursework
    PULL_STUDENTS = os.getenv("PULL_STUDENTS") == "YES" or args.students
    PULL_TEACHERS = os.getenv("PULL_TEACHERS") == "YES" or args.teachers
    PULL_GUARDIANS = os.getenv("PULL_GUARDIANS") == "YES" or args.guardians
    PULL_SUBMISSIONS = os.getenv("PULL_SUBMISSIONS") == "YES" or args.submissions
    PULL_GUARDIAN_INVITES = os.getenv("PULL_GUARDIAN_INVITES") == "YES" or args.invites


class Test_Config(Config):
    DB_TYPE = "sqlite"
    DEBUG = False
    PULL_USAGE = True
    PULL_COURSES = True
    PULL_TOPICS = True
    PULL_COURSEWORK = True
    PULL_STUDENTS = True
    PULL_TEACHERS = True
    PULL_GUARDIANS = True
    PULL_SUBMISSIONS = True
    PULL_GUARDIAN_INVITES = True


def db_generator(config):
    db_type = config.DB_TYPE
    if db_type == "mssql":
        return MSSQL()
    elif db_type == "postgres":
        return PostgreSQL()
    elif db_type == "sqlite":
        return SQLite()
    else:
        raise Exception()
