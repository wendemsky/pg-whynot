-- Q3: 2-way JOIN with filter
-- Tests the ⋈ operator.  items joined with stocks on item ID,
-- with a quantity threshold filter.
--
-- Expected missing tuple examples:
--
--   Example A — JOIN PARTNER MISSING:
--     (i_name='Dove')
--     Cause: item 339 (Dove) has NO rows in stocks at all
--            → join produces nothing for this item
--
--   Example B — PREDICATE FAILED (after join):
--     (i_name='Indapamide', w_id=301, s_qty=338)
--     Cause: stock (w=301, i=1) exists and joins successfully,
--            but s_qty=338 fails s_qty > 500  (gap: -162)

SELECT i.i_name, s.w_id, s.s_qty
FROM items i JOIN stocks s ON i.i_id = s.i_id
WHERE s.s_qty > 500
