-- Q4: UNION
-- Tests the ∪ operator.  Retrieves warehouses in Singapore OR Malaysia.
--
-- Expected missing tuple example:
--   (w_id=1, w_name='DabZ')   — warehouse 1 is in Indonesia
--   Cause: absent from both UNION branches
--     Branch 1: w_country='Indonesia' ≠ 'Singapore'
--     Branch 2: w_country='Indonesia' ≠ 'Malaysia'
--
-- A tuple that appears in BOTH branches (if any) will have its
-- annotation combined via semiring.add() — visible in the How semiring
-- as a sum of two monomials.

SELECT w_id, w_name, w_country
FROM warehouses
WHERE w_country = 'Singapore'
UNION
SELECT w_id, w_name, w_country
FROM warehouses
WHERE w_country = 'Malaysia'
