import pandas as pd

# COURSE SYNC DATA
# Note on the data:
# They are designed to test a few different scenarios:
# 1. The "normal" flows.
# 2. Course data that is missing alias data (Physics should be ignored).
# 3. Alias data that is missing course data (d:111 should be ignored).
# 4. Archived courses (Paleontology should be ignored).

COURSE_DATA = pd.DataFrame(
    {
        "id": ["1", "2", "3", "4", "5"],
        "name": ["Biology", "Math", "English", "Paleontology", "Physics"],
        "courseState": ["ACTIVE", "ACTIVE", "ACTIVE", "ARCHIVED", "ACTIVE"],
        "section": ["1", "1", "2", "3", "4"],
    }
)
ALIAS_DATA = pd.DataFrame(
    {
        "courseId": ["1", "2", "3", "4", "0"],
        "alias": ["d:123", "d:234", "d:345", "d:456", "d:111"],
    }
)
COURSE_SYNC_DATA = pd.DataFrame(
    {
        "alias": ["123", "234", "678", "789"],
        "name": ["Biology", "Math", "History", "Computer Science"],
        "section": ["1", "1", "2", "2"],
        "teacher_email": ["a@b.com", "a@b.com", "a@b.com", "a@b.com"],
    }
)

TO_CREATE_COURSE_SOLUTION = pd.DataFrame(
    {
        "alias": ["d:678", "d:789"],
        "name": ["History", "Computer Science"],
        "section": ["2", "2"],
        "teacher_email": ["a@b.com", "a@b.com"],
    }
)
TO_DELETE_COURSE_SOLUTION = pd.DataFrame(
    {"alias": ["d:345"], "name": ["English"], "section": ["2"], "courseId": ["3"]}
)

# STUDENT SYNC DATA

STUDENT_DATA = pd.DataFrame(
    {
        "courseId": ["1", "1", "2", "2"],
        "userId": ["1", "2", "1", "3"],
        "fullName": ["User1", "User2", "User1", "User3"],
        "emailAddress": ["1@a.com", "2@a.com", "1@a.com", "3@a.com"],
    }
)

STUDENT_SYNC_DATA = pd.DataFrame(
    {
        "alias": ["123", "234", "345"],
        "emailAddress": ["1@a.com", "2@a.com", "1@a.com"],
    }
)

TO_CREATE_STUDENT_SOLUTION = pd.DataFrame(
    {
        "alias": ["d:234", "d:345"],
        "emailAddress": ["2@a.com", "1@a.com"],
    }
)

TO_DELETE_STUDENT_SOLUTION = pd.DataFrame(
    {
        "alias": ["d:123", "d:234", "d:234"],
        "emailAddress": ["2@a.com", "1@a.com", "3@a.com"],
        "courseId": ["1", "2", "2"],
        "userId": ["2", "1", "3"],
        "fullName": ["User2", "User1", "User3"],
    }
)

# TEACHER SYNC DATA

TEACHER_DATA = pd.DataFrame(
    {
        "courseId": ["1", "1", "2", "2"],
        "userId": ["91", "92", "91", "93"],
        "fullName": ["Teacher1", "Teacher2", "Teacher1", "Teacher3"],
        "emailAddress": ["t1@a.com", "t2@a.com", "t1@a.com", "t3@a.com"],
    }
)

TEACHER_SYNC_DATA = pd.DataFrame(
    {
        "alias": ["123", "234", "345"],
        "emailAddress": ["t1@a.com", "t2@a.com", "t1@a.com"],
    }
)

TO_CREATE_TEACHER_SOLUTION = pd.DataFrame(
    {
        "alias": ["d:234", "d:345"],
        "emailAddress": ["t2@a.com", "t1@a.com"],
    }
)

TO_DELETE_TEACHER_SOLUTION = pd.DataFrame(
    {
        "alias": ["d:123", "d:234", "d:234"],
        "emailAddress": ["t2@a.com", "t1@a.com", "t3@a.com"],
        "courseId": ["1", "2", "2"],
        "userId": ["92", "91", "93"],
        "fullName": ["Teacher2", "Teacher1", "Teacher3"],
    }
)
