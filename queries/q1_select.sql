-- Q1: SELECT / filter
-- Tests the σ (sigma) operator with a single WHERE predicate.
--
-- Expected missing tuple example:
--   (i_id=3, i_name='Meprobamate', i_price=11.64)
--   Cause: i_price=11.64 fails the predicate i_price > 50
--
-- Why-Not question:
--   "Why is item 'Meprobamate' not in the result?"
--   → Predicate failed: i_price > 50  (actual: 11.64, gap: -38.36)

SELECT i_id, i_name, i_price
FROM items
WHERE i_price > 50
