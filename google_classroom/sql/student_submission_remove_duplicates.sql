WITH
    duplicates
    AS
    (
        SELECT
            id, assignedGrade, updateTime,
            ROW_NUMBER() OVER (
            PARTITION BY
                id
            ORDER BY
                updateTime
            DESC
        ) row_num
        FROM GoogleClassroom_StudentSubmissions
    )
DELETE
FROM duplicates
WHERE row_num > 1;