-- ============================================================
-- Load credits into the OLAP fact table.
--
-- Grain:
-- one title + person + role + character relationship
--
-- OLTP natural/business keys are translated into
-- OLAP surrogate keys through the dimensions.
-- ============================================================

INSERT INTO analytics.fact_credits (
    title_sk,
    person_sk,
    role,
    character
)

SELECT
    dt.title_sk,
    dp.person_sk,
    c.role,
    c.character

FROM public.credit AS c

INNER JOIN analytics.dim_title AS dt
    ON dt.title_id = c.title_id

INNER JOIN analytics.dim_person AS dp
    ON dp.person_id = c.person_id

WHERE c.title_id IS NOT NULL
  AND c.person_id IS NOT NULL
  AND c.role IS NOT NULL

ON CONFLICT DO NOTHING;