-- Expected: zero rows. Check interval continuity and absence of overlap.
WITH ordered AS (
  SELECT *, LEAD(valid_from) OVER (PARTITION BY entity_id ORDER BY valid_from) AS next_start
  FROM customer_history
)
SELECT entity_id, valid_from, valid_to, next_start
FROM ordered
WHERE (next_start IS NOT NULL AND (valid_to IS NULL OR valid_to <> next_start))
   OR (next_start IS NULL AND valid_to IS NOT NULL);
