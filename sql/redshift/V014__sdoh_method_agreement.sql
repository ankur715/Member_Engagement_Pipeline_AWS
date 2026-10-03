-- How often the LLM tagger agrees with the rule-based tagger, per need
-- category, over notes both methods have classified. Counts only (no PHI).
-- agreement_pct = both / (both + rules_only + llm_only)  (Jaccard overlap).
-- Reviewing this is the gate before LLM tags are used for decisions; the
-- vulnerability index keeps using the rule-based tags.
CREATE OR REPLACE VIEW analytics.v_sdoh_method_agreement AS
WITH compared AS (
    SELECT r.activity_id
    FROM core.note_classifications r
    JOIN core.note_classifications l ON l.activity_id = r.activity_id AND l.method = 'llm'
    WHERE r.method = 'rules'
),
tags AS (
    SELECT n.activity_id, n.need_category,
           MAX(CASE WHEN n.method = 'rules' THEN 1 ELSE 0 END) AS by_rules,
           MAX(CASE WHEN n.method = 'llm' THEN 1 ELSE 0 END) AS by_llm
    FROM core.member_sdoh_needs n
    JOIN compared c ON c.activity_id = n.activity_id
    GROUP BY 1, 2
),
totals AS (
    SELECT COUNT(*) AS notes_compared FROM compared
)
SELECT t.need_category,
       MAX(totals.notes_compared) AS notes_compared,
       SUM(CASE WHEN by_rules = 1 AND by_llm = 1 THEN 1 ELSE 0 END) AS both_methods,
       SUM(CASE WHEN by_rules = 1 AND by_llm = 0 THEN 1 ELSE 0 END) AS rules_only,
       SUM(CASE WHEN by_rules = 0 AND by_llm = 1 THEN 1 ELSE 0 END) AS llm_only,
       ROUND(100.0 * SUM(CASE WHEN by_rules = 1 AND by_llm = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1)
           AS agreement_pct
FROM tags t
CROSS JOIN totals
GROUP BY t.need_category;

GRANT SELECT ON analytics.v_sdoh_method_agreement TO ROLE analyst_ro;
GRANT SELECT ON analytics.v_sdoh_method_agreement TO ROLE data_science_ro;
