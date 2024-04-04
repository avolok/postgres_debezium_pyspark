DELETE FROM cdctable c 
WHERE id IN (
	SELECT id FROM cdctable d
	ORDER BY random()
	LIMIT 1
)
RETURNING id as deleted_id;