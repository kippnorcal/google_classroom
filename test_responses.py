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
        "profile.name.fullName": ["Test User", "Another User"],
        "profile.emailAddress": ["test_user@email.com", "another_user@email.com"],
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
        "profile.name.fullName": ["Boss Lady", "Mr. Teacher", "Mrs. Teacher"],
        "profile.emailAddress": [
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


class FakeBatchRequest:
    def __init__(self, callback):
        self.callback = callback
        self.requests = []

    def add(self, request, request_id):
        self.requests.append((request, request_id))

    def execute(self):
        for (request, request_id) in self.requests:
            result = request.execute()
            self.callback(request_id, result, None)


class FakeRequest:
    def __init__(self, result, *args, **kwargs):
        self.result = result
        self.args = args
        self.kwargs = kwargs

    def execute(self):
        if "courseId" in self.kwargs:
            course_id = self.kwargs["courseId"]
            if course_id is not None:
                # If a course_id is provided, this splits the results into two courses.
                key = list(self.result.keys())[0]
                if course_id == 0:
                    return {key: self.result[key][:1]}
                else:
                    return {key: self.result[key][1:]}
        return self.result


class FakeEndpoint:
    def __init__(self, result):
        self.result = result

    def list(self, *args, **kwargs):
        return FakeRequest(self.result, *args, **kwargs)


class FakeService:
    class Courses(FakeEndpoint):
        def topics(self):
            return FakeEndpoint(TOPIC_RESPONSE)

        def students(self):
            return FakeEndpoint(STUDENT_RESPONSE)

        def teachers(self):
            return FakeEndpoint(TEACHER_RESPONSE)

        def aliases(self):
            return FakeEndpoint(ALIAS_RESPONSE)

        def announcements(self):
            return FakeEndpoint(ANNOUNCEMENT_RESPONSE)

    def courses(self):
        return self.Courses(COURSE_RESPONSE)

    def orgunits(self):
        return FakeEndpoint(ORG_UNIT_RESPONSE)

    def invitations(self):
        return FakeEndpoint(INVITATION_RESPONSE)

    def new_batch_http_request(self, callback):
        return FakeBatchRequest(callback)

    class UserProfiles:
        def guardians(self):
            return FakeEndpoint(GUARDIAN_RESPONSE)

        def guardianInvitations(self):
            return FakeEndpoint(GUARDIAN_INVITE_RESPONSE)

    def userProfiles(self):
        return self.UserProfiles()
