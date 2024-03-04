MERGE INTO cdctable t USING (
    select
        %s as id,
        %s as name,
        cast(%s as date) as "date"
) s ON s.id = t.id
WHEN MATCHED THEN
UPDATE
SET
    name = s.name,
    "date" = s."date"
    WHEN NOT MATCHED THEN
INSERT
    (id, name, "date")
VALUES
    (s.id, s.name, s."date");