-- Q2: PROJECT + SELECT
-- Tests the π (pi) operator — the SELECT column list restricts output columns.
--
-- Expected missing tuple example:
--   (i_name='Meprobamate', i_price=11.64)
--   Cause: filtered by i_price > 30  (actual: 11.64)
--
-- Also illustrates projection: i_id and i_im_id are projected away.
-- If a user asks "why is i_id=3 missing?" → PROJECTION_HIDDEN cause.

SELECT i_name, i_price
FROM items
WHERE i_price > 30
