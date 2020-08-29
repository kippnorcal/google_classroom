import pandas as pd

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
SOURCE_DATA = pd.DataFrame(
    {
        "alias": ["123", "234", "678", "789"],
        "name": ["Biology", "Math", "History", "Computer Science"],
        "section": ["1", "1", "2", "2"],
        "teacher_email": ["a@b.com", "a@b.com", "a@b.com", "a@b.com"],
    }
)

TO_CREATE_SOLUTION = pd.DataFrame(
    {
        "id": ["d:678", "d:789"],
        "name": ["History", "Computer Science"],
        "section": ["2", "2"],
        "ownerId": ["a@b.com", "a@b.com"],
    }
)
TO_DELETE_SOLUTION = pd.DataFrame(
    {"courseId": ["3"], "name": ["English"], "section": ["2"], "alias": ["d:345"]}
)
