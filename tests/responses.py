import pandas as pd

ORG_UNIT_SOLUTION = pd.DataFrame(
    {
        "name": ["Test Organization 2"],
        "description": ["Description 2"],
        "orgUnitPath": ["/TestOrg2"],
        "orgUnitId": ["id:987654"],
    }
)
ORG_UNIT_RESPONSE = {
    "organizationUnits": [
        {
            "kind": "admin#directory#orgUnit",
            "etag": '"abcdef"',
            "name": "Test Organization",
            "description": "Description",
            "orgUnitPath": "/TestOrg",
            "orgUnitId": "id:123456",
            "parentOrgUnitPath": "/",
            "parentOrgUnitId": "id:234567",
        },
        {
            "kind": "admin#directory#orgUnit",
            "etag": '"qwerty"',
            "name": "Test Organization 2",
            "description": "Description 2",
            "orgUnitPath": "/TestOrg2",
            "orgUnitId": "id:987654",
            "parentOrgUnitPath": "/",
            "parentOrgUnitId": "id:876543",
        },
    ]
}

GUARDIAN_SOLUTION = pd.DataFrame(
    {
        "studentId": ["12345", "33333"],
        "guardianId": ["23456", "44444"],
        "invitedEmailAddress": ["anotherName@email.com", "aname2@email.com"],
    }
)
GUARDIAN_RESPONSE = {
    "guardians": [
        {
            "studentId": "12345",
            "guardianId": "23456",
            "guardianProfile": {
                "id": "999888777",
                "name": {
                    "givenName": "First",
                    "familyName": "Last",
                    "fullName": "First Last",
                },
                "emailAddress": "name@email.com",
            },
            "invitedEmailAddress": "anotherName@email.com",
        },
        {
            "studentId": "33333",
            "guardianId": "44444",
            "guardianProfile": {
                "id": "1111111333",
                "name": {
                    "givenName": "Another",
                    "familyName": "Name",
                    "fullName": "Another Name",
                },
                "emailAddress": "aname@email.com",
            },
            "invitedEmailAddress": "aname2@email.com",
        },
    ]
}

GUARDIAN_INVITE_SOLUTION = pd.DataFrame(
    {
        "studentId": ["12345", "333"],
        "invitationId": ["1", "2"],
        "invitedEmailAddress": ["name@email.com", "another_name@email.com"],
        "state": ["COMPLETE", "COMPLETE"],
        "creationTime": [
            pd.to_datetime("2020-04-05 19:42:29.966"),
            pd.to_datetime("2020-05-05 19:42:29.966"),
        ],
    }
)
GUARDIAN_INVITE_RESPONSE = {
    "guardianInvitations": [
        {
            "studentId": "12345",
            "invitationId": "1",
            "invitedEmailAddress": "name@email.com",
            "state": "COMPLETE",
            "creationTime": "2020-04-05T19:42:29.966Z",
        },
        {
            "studentId": "333",
            "invitationId": "2",
            "invitedEmailAddress": "another_name@email.com",
            "state": "COMPLETE",
            "creationTime": "2020-05-05T19:42:29.966Z",
        },
    ]
}

COURSE_SOLUTION = pd.DataFrame(
    {
        "id": ["12345", "23456"],
        "name": ["Science Class", "Math Class"],
        "courseGroupEmail": ["science@class.com", "math@class.com"],
        "courseState": ["ACTIVE", "ACTIVE"],
        "creationTime": [
            pd.to_datetime("2020-04-05 19:41:15.292"),
            pd.to_datetime("2020-04-01 17:44:34.899"),
        ],
        "description": ["Class description 1", "Class description 2"],
        "descriptionHeading": ["Science Class", "Math Class"],
        "enrollmentCode": ["abcdefg", "ghijklmnop"],
        "guardiansEnabled": [True, True],
        "ownerId": ["123", "234"],
        "room": ["Room1", "Room2"],
        "section": ["1", "2"],
        "teacherGroupEmail": ["science_teachers@class.com", "math_teachers@class.com"],
        "updateTime": [
            pd.to_datetime("2020-04-05 19:41:14.305"),
            pd.to_datetime("2020-04-01 20:54:08.531"),
        ],
    }
)
COURSE_RESPONSE = {
    "courses": [
        {
            "id": "12345",
            "name": "Science Class",
            "courseGroupEmail": "science@class.com",
            "courseState": "ACTIVE",
            "creationTime": "2020-04-05T19:41:15.292Z",
            "description": "Class description 1",
            "descriptionHeading": "Science Class",
            "enrollmentCode": "abcdefg",
            "guardiansEnabled": True,
            "ownerId": "123",
            "room": "Room1",
            "section": "1",
            "teacherGroupEmail": "science_teachers@class.com",
            "updateTime": "2020-04-05T19:41:14.305Z",
        },
        {
            "id": "23456",
            "name": "Math Class",
            "courseGroupEmail": "math@class.com",
            "courseState": "ACTIVE",
            "creationTime": "2020-04-01T17:44:34.899Z",
            "description": "Class description 2",
            "descriptionHeading": "Math Class",
            "enrollmentCode": "ghijklmnop",
            "guardiansEnabled": True,
            "ownerId": "234",
            "room": "Room2",
            "section": "2",
            "teacherGroupEmail": "math_teachers@class.com",
            "updateTime": "2020-04-01T20:54:08.531Z",
        },
    ]
}

ALIAS_SOLUTION = pd.DataFrame({"courseId": [None], "alias": ["d:school_test1"]})
ALIAS_RESPONSE = {"aliases": [{"alias": "d:school_test1"}]}

INVITATION_SOLUTION = pd.DataFrame(
    {
        "id": ["12345", "23456"],
        "userId": ["1", "2"],
        "courseId": ["1234", "5678"],
        "role": ["STUDENT", "STUDENT"],
    }
)
INVITATION_RESPONSE = {
    "invitations": [
        {"id": "12345", "userId": "1", "courseId": "1234", "role": "STUDENT"},
        {"id": "23456", "userId": "2", "courseId": "5678", "role": "STUDENT"},
    ]
}

ANNOUNCEMENT_SOLUTION = pd.DataFrame(
    {
        "id": ["12345", "23456"],
        "courseId": ["1234", "5678"],
        "text": ["Test Announcement #1", "Test Announcement #2"],
        "state": ["PUBLISHED", "PUBLISHED"],
        "alternateLink": [
            "https://classroom.google.com/c/Abc1DeF2Gh",
            "https://classroom.google.com/c/Bcd2EfG3Hi",
        ],
        "creationTime": [
            pd.to_datetime("2020-04-05 19:41:15.292"),
            pd.to_datetime("2020-04-05 19:41:15.292"),
        ],
        "updateTime": [
            pd.to_datetime("2020-04-05 19:41:14.305"),
            pd.to_datetime("2020-04-05 19:41:14.305"),
        ],
        "scheduledTime": [
            pd.to_datetime("2020-04-06 00:00:00.000"),
            pd.to_datetime("2020-04-06 00:00:00.000"),
        ],
        "assigneeMode": ["ALL_STUDENTS", "ALL_STUDENTS"],
        "creatorUserId": ["555", "333"],
    }
)
ANNOUNCEMENT_RESPONSE = {
    "announcements": [
        {
            "courseId": "1234",
            "id": "12345",
            "text": "Test Announcement #1",
            "materials": [],
            "state": "PUBLISHED",
            "alternateLink": "https://classroom.google.com/c/Abc1DeF2Gh",
            "creationTime": "2020-04-05T19:41:15.292Z",
            "updateTime": "2020-04-05T19:41:14.305Z",
            "scheduledTime": "2020-04-06T00:00:00.000Z",
            "assigneeMode": "ALL_STUDENTS",
            "individualStudentsOptions": {},
            "creatorUserId": "555",
        },
        {
            "courseId": "5678",
            "id": "23456",
            "text": "Test Announcement #2",
            "materials": [],
            "state": "PUBLISHED",
            "alternateLink": "https://classroom.google.com/c/Bcd2EfG3Hi",
            "creationTime": "2020-04-05T19:41:15.292Z",
            "updateTime": "2020-04-05T19:41:14.305Z",
            "scheduledTime": "2020-04-06T00:00:00.000Z",
            "assigneeMode": "ALL_STUDENTS",
            "individualStudentsOptions": {},
            "creatorUserId": "333",
        },
    ]
}

TOPIC_SOLUTION = pd.DataFrame(
    {
        "courseId": ["1234", "5678"],
        "topicId": ["1235", "1234"],
        "name": ["Chemistry", "Biology"],
        "updateTime": [
            pd.to_datetime("2020-04-05 22:41:55.871"),
            pd.to_datetime("2020-04-05 22:41:49.187"),
        ],
    }
)
TOPIC_RESPONSE = {
    "topic": [
        {
            "courseId": "1234",
            "topicId": "1235",
            "name": "Chemistry",
            "updateTime": "2020-04-05T22:41:55.871Z",
        },
        {
            "courseId": "5678",
            "topicId": "1234",
            "name": "Biology",
            "updateTime": "2020-04-05T22:41:49.187Z",
        },
    ]
}

STUDENT_SOLUTION = pd.DataFrame(
    {
        "courseId": ["123", "123"],
        "userId": ["1", "2"],
        "fullName": ["Test User", "Another User"],
        "emailAddress": ["test_user@email.com", "another_user@email.com"],
    }
)
STUDENT_RESPONSE = {
    "students": [
        {
            "courseId": "123",
            "userId": "1",
            "profile": {
                "id": "222",
                "name": {
                    "givenName": "Test",
                    "familyName": "User",
                    "fullName": "Test User",
                },
                "emailAddress": "test_user@email.com",
            },
        },
        {
            "courseId": "123",
            "userId": "2",
            "profile": {
                "id": "333",
                "name": {
                    "givenName": "Another",
                    "familyName": "User",
                    "fullName": "Another User",
                },
                "emailAddress": "another_user@email.com",
                "permissions": [{"permission": "CREATE_COURSE"}],
            },
        },
    ]
}

TEACHER_SOLUTION = pd.DataFrame(
    {
        "courseId": ["111", "333", "444"],
        "userId": ["555", "321", "555"],
        "fullName": ["Boss Lady", "Mr. Teacher", "Mrs. Teacher"],
        "emailAddress": [
            "boss_lady@email.com",
            "mr_teacher@email.com",
            "mrs_teacher@email.com",
        ],
    }
)
TEACHER_RESPONSE = {
    "teachers": [
        {
            "courseId": "111",
            "userId": "555",
            "profile": {
                "id": "987",
                "name": {
                    "givenName": "Boss",
                    "familyName": "Lady",
                    "fullName": "Boss Lady",
                },
                "emailAddress": "boss_lady@email.com",
                "permissions": [{"permission": "CREATE_COURSE"}],
            },
        },
        {
            "courseId": "333",
            "userId": "321",
            "profile": {
                "id": "543",
                "name": {
                    "givenName": "Mr.",
                    "familyName": "Teacher",
                    "fullName": "Mr. Teacher",
                },
                "emailAddress": "mr_teacher@email.com",
                "permissions": [{"permission": "CREATE_COURSE"}],
            },
        },
        {
            "courseId": "444",
            "userId": "555",
            "profile": {
                "id": "789",
                "name": {
                    "givenName": "Mrs.",
                    "familyName": "Teacher",
                    "fullName": "Mrs. Teacher",
                },
                "emailAddress": "mrs_teacher@email.com",
                "permissions": [{"permission": "CREATE_COURSE"}],
            },
        },
    ]
}

STUDENT_SUBMISSION_SOLUTION = pd.DataFrame(
    {
        "courseId": ["1", "2"],
        "courseWorkId": ["123", "456"],
        "id": ["abc", "def"],
        "userId": ["6", "7"],
        "creationTime": [
            pd.to_datetime("2020-04-05 19:41:15.292"),
            pd.to_datetime("2020-04-01 17:44:34.899"),
        ],
        "updateTime": [
            pd.to_datetime("2020-04-06 19:41:15.292"),
            pd.to_datetime("2020-04-02 17:44:34.899"),
        ],
        "state": ["RETURNED", "RETURNED"],
        "draftGrade": [30, 80],
        "assignedGrade": [40, 95],
        "courseWorkType": ["ASSIGNMENT", "ASSIGNMENT"],
        "createdTime": [
            pd.to_datetime("2020-04-02 19:41:15.292"),
            pd.to_datetime("2020-04-01 17:44:34.899"),
        ],
        "turnedInTimestamp": [
            pd.to_datetime("2020-04-09 19:41:15.292"),
            pd.to_datetime("2020-04-08 17:44:34.899"),
        ],
        "returnedTimestamp": [
            pd.to_datetime("2020-04-10 19:41:15.292"),
            pd.to_datetime("2020-04-09 17:44:34.899"),
        ],
        "draftMaxPoints": [100, 100],
        "draftGradeTimestamp": [
            pd.to_datetime("2020-04-11 19:41:15.292"),
            pd.to_datetime("2020-04-10 17:44:34.899"),
        ],
        "draftGraderId": ["80", "80"],
        "assignedMaxPoints": [100, 100],
        "assignedGradeTimestamp": [
            pd.to_datetime("2020-04-12 19:41:15.292"),
            pd.to_datetime("2020-04-11 17:44:34.899"),
        ],
        "assignedGraderId": ["90", "90"],
    }
)
STUDENT_SUBMISSION_RESPONSE = {
    "studentSubmissions": [
        {
            "courseId": "1",
            "courseWorkId": "123",
            "id": "abc",
            "userId": "6",
            "creationTime": "2020-04-05T19:41:15.292Z",
            "updateTime": "2020-04-06T19:41:15.292Z",
            "state": "RETURNED",
            "draftGrade": 30,
            "assignedGrade": 40,
            "courseWorkType": "ASSIGNMENT",
            "submissionHistory": [
                {
                    "stateHistory": {
                        "state": "CREATED",
                        "stateTimestamp": "2020-04-02T19:41:15.292Z",
                        "actorUserId": "1",
                    }
                },
                {
                    "stateHistory": {
                        "state": "TURNED_IN",
                        "stateTimestamp": "2020-04-09T19:41:15.292Z",
                        "actorUserId": "1",
                    }
                },
                {
                    "gradeHistory": {
                        "maxPoints": 100,
                        "gradeTimestamp": "2020-04-11T19:41:15.292Z",
                        "actorUserId": "80",
                        "gradeChangeType": "DRAFT_GRADE_POINTS_EARNED_CHANGE",
                    }
                },
                {
                    "stateHistory": {
                        "state": "RETURNED",
                        "stateTimestamp": "2020-04-10T19:41:15.292Z",
                        "actorUserId": "1",
                    }
                },
                {
                    "gradeHistory": {
                        "maxPoints": 100,
                        "gradeTimestamp": "2020-04-12T19:41:15.292Z",
                        "actorUserId": "90",
                        "gradeChangeType": "ASSIGNED_GRADE_POINTS_EARNED_CHANGE",
                    }
                },
            ],
        },
        {
            "courseId": "2",
            "courseWorkId": "456",
            "id": "def",
            "userId": "7",
            "creationTime": "2020-04-01T17:44:34.899Z",
            "updateTime": "2020-04-02T17:44:34.899Z",
            "state": "RETURNED",
            "draftGrade": 80,
            "assignedGrade": 95,
            "courseWorkType": "ASSIGNMENT",
            "submissionHistory": [
                {
                    "stateHistory": {
                        "state": "CREATED",
                        "stateTimestamp": "2020-04-01T17:44:34.899Z",
                    }
                },
                {
                    "stateHistory": {
                        "state": "TURNED_IN",
                        "stateTimestamp": "2020-04-08T17:44:34.899Z",
                    }
                },
                {
                    "gradeHistory": {
                        "pointsEarned": 80,
                        "maxPoints": 100,
                        "gradeTimestamp": "2020-04-10T17:44:34.899Z",
                        "actorUserId": "80",
                        "gradeChangeType": "DRAFT_GRADE_POINTS_EARNED_CHANGE",
                    }
                },
                {
                    "stateHistory": {
                        "state": "RETURNED",
                        "stateTimestamp": "2020-04-09T17:44:34.899Z",
                    }
                },
                {
                    "gradeHistory": {
                        "pointsEarned": 95,
                        "maxPoints": 100,
                        "gradeTimestamp": "2020-04-11T17:44:34.899Z",
                        "actorUserId": "90",
                        "gradeChangeType": "ASSIGNED_GRADE_POINTS_EARNED_CHANGE",
                    }
                },
            ],
        },
    ]
}

COURSEWORK_SOLUTION = pd.DataFrame(
    {
        "courseId": ["1", "2"],
        "id": ["3", "4"],
        "title": ["title1", "title2"],
        "description": ["desc1", "desc2"],
        "state": ["PUBLISHED", "DRAFT"],
        "alternateLink": ["link1", "link2"],
        "creationTime": [
            pd.to_datetime("2020-04-25 19:26:58.798"),
            pd.to_datetime("2020-04-26 14:26:58.798"),
        ],
        "updateTime": [
            pd.to_datetime("2020-04-27 19:28:20.824"),
            pd.to_datetime("2020-04-28 14:28:20.824"),
        ],
        "dueDate": [None, pd.to_datetime("2020-05-01 06:59:00")],
        "maxPoints": [100, 100],
        "workType": ["ASSIGNMENT", "QUIZ"],
        "assigneeMode": ["ALL_STUDENTS", "ALL_STUDENTS"],
        "submissionModificationMode": [
            "MODIFIABLE_UNTIL_TURNED_IN",
            "MODIFIABLE_UNTIL_TURNED_IN",
        ],
        "creatorUserId": ["60", "70"],
        "topicId": [None, "20"],
    }
)
COURSEWORK_RESPONSE = {
    "courseWork": [
        {
            "courseId": "1",
            "id": "3",
            "title": "title1",
            "description": "desc1",
            "state": "PUBLISHED",
            "alternateLink": "link1",
            "creationTime": "2020-04-25T19:26:58.798Z",
            "updateTime": "2020-04-27T19:28:20.824Z",
            "maxPoints": 100,
            "workType": "ASSIGNMENT",
            "submissionModificationMode": "MODIFIABLE_UNTIL_TURNED_IN",
            "assigneeMode": "ALL_STUDENTS",
            "creatorUserId": "60",
        },
        {
            "courseId": "2",
            "id": "4",
            "title": "title2",
            "description": "desc2",
            "state": "DRAFT",
            "alternateLink": "link2",
            "creationTime": "2020-04-26T14:26:58.798Z",
            "updateTime": "2020-04-28T14:28:20.824Z",
            "dueDate": {"year": 2020, "month": 5, "day": 1},
            "dueTime": {"hours": 6, "minutes": 59},
            "maxPoints": 100,
            "workType": "QUIZ",
            "submissionModificationMode": "MODIFIABLE_UNTIL_TURNED_IN",
            "assigneeMode": "ALL_STUDENTS",
            "creatorUserId": "70",
            "topicId": "20",
        },
    ]
}

STUDENT_USAGE_SOLUTION = pd.DataFrame(
    {
        "Email": ["user1@email.com", "user2@email.com", "user3@email.com"],
        "AsOfDate": [
            pd.to_datetime("2020-02-27 00:00:00"),
            pd.to_datetime("2020-02-27 00:00:00"),
            pd.to_datetime("2020-02-28 00:00:00"),
        ],
        "LastUsedTime": [
            pd.to_datetime("2020-02-27 18:11:01"),
            pd.to_datetime("1970-01-01 00:00:00"),
            pd.to_datetime("2020-02-14 20:29:34"),
        ],
        "ImportDate": [
            pd.to_datetime("today").normalize(),
            pd.to_datetime("today").normalize(),
            pd.to_datetime("today").normalize(),
        ],
    }
)
STUDENT_USAGE_RESPONSE = {
    "2020-02-27": {
        "usageReports": [
            {
                "date": "2020-02-27",
                "entity": {"userEmail": "user1@email.com"},
                "parameters": [
                    {
                        "name": "classroom:last_interaction_time",
                        "datetimeValue": "2020-02-27T18:11:01.000Z",
                    }
                ],
            },
            {
                "date": "2020-02-27",
                "entity": {"userEmail": "user2@email.com"},
                "parameters": [
                    {
                        "name": "classroom:last_interaction_time",
                        "datetimeValue": "1970-01-01T00:00:00.000Z",
                    }
                ],
            },
        ],
    },
    "2020-02-28": {
        "usageReports": [
            {
                "date": "2020-02-28",
                "entity": {"userEmail": "user3@email.com"},
                "parameters": [
                    {
                        "name": "classroom:last_interaction_time",
                        "datetimeValue": "2020-02-14T20:29:34.000Z",
                    }
                ],
            },
        ],
    },
}
