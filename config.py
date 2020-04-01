courses = {
    "columns": [
        "id",
        "name",
        "courseGroupEmail",
        "courseState",
        "creationTime",
        "description",
        "descriptionHeading",
        "enrollmentCode",
        "guardiansEnabled",
        "ownerId",
        "room",
        "section",
        "teacherGroupEmail",
        "updateTime",
    ],
    "date_columns": ["creationTime", "updateTime"],
}

topics = {
    "columns": ["courseId", "topicId", "name", "updateTime"],
    "date_columns": ["updateTime"],
}

students = {
    "columns": ["courseId", "userId", "profile.fullName", "emailAddress"],
    "date_columns": [],
}

teachers = {
    "columns": ["courseId", "userId", "profile.fullName", "emailAddress"],
    "date_columns": [],
}

coursework = {
    "columns": [
        "courseId",
        "id",
        "title",
        "description",
        "state",
        "alternateLink",
        "creationTime",
        "updateTime",
        "dueDate",  # requires further parsing
        "dueTime",  # requires further parsing
        "scheduledTime",
        "maxPoints",
        "workType",
        "assigneeMode",
        "submissionModificationMode",
        "creatorUserId",
        "topicId",
    ],
    "date_columns": ["creationTime", "updateTime", "dueDate"],
}

student_submissions = {
    "columns": [
        "courseId",
        "courseWorkId",
        "id",
        "userId",
        "creationTime",
        "updateTime",
        "state",
        "late",
        "draftGrade",
        "assignedGrade",
        "alternateLink",
        "courseWorkType",
        "submissionHistory",  # requires further parsing
    ],
    "date_columns": ["creationTime", "updateTime"],
}

guardians = {
    "columns": ["studentId", "guardianId", "invitedEmailAddress"],
    "date_columns": [],
}

guardian_invites = {
    "columns": [
        "studentId",
        "invitationId",
        "invitedEmailAddress",
        "state",
        "creationTime",
    ],
    "date_columns": ["creationTime"],
}
