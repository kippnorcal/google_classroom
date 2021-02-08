WITH duplicates AS (
    SELECT
        "id",
        "uniqueId",
        ROW_NUMBER() OVER (
            PARTITION BY "id"
            ORDER BY "updateTime" DESC
        ) row_num
    FROM {schema}."GoogleClassroom_StudentSubmissions"
)
DELETE
FROM {schema}."GoogleClassroom_StudentSubmissions"
WHERE "uniqueId" in (select "uniqueId" from duplicates where "row_num" > 1);