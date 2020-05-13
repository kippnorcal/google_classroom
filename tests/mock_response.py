from tests.responses import (
    ALIAS_RESPONSE,
    ANNOUNCEMENT_RESPONSE,
    COURSE_RESPONSE,
    COURSEWORK_RESPONSE,
    GUARDIAN_INVITE_RESPONSE,
    GUARDIAN_RESPONSE,
    INVITATION_RESPONSE,
    ORG_UNIT_RESPONSE,
    STUDENT_RESPONSE,
    STUDENT_SUBMISSION_RESPONSE,
    STUDENT_USAGE_RESPONSE,
    TEACHER_RESPONSE,
    TOPIC_RESPONSE,
)


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
        if "date" in self.kwargs:
            date = self.kwargs["date"]
            return self.result.get(date)
        return self.result


class FakeEndpoint:
    def __init__(self, result):
        self.result = result

    def list(self, *args, **kwargs):
        return FakeRequest(self.result, *args, **kwargs)

    def get(self, *args, **kwargs):
        return FakeRequest(self.result, *args, **kwargs)


class FakeService:
    class Courses(FakeEndpoint):
        class CourseWork(FakeEndpoint):
            def studentSubmissions(self):
                return FakeEndpoint(STUDENT_SUBMISSION_RESPONSE)

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

        def courseWork(self):
            return self.CourseWork(COURSEWORK_RESPONSE)

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

    def userUsageReport(self):
        return FakeEndpoint(STUDENT_USAGE_RESPONSE)
