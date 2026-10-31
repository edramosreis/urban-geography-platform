-- dbt fails this test when it returns any invalid aggregate row.
WITH summaries AS (
    SELECT 'grid' AS model, mapped_buildings, buildings_within, share_within
    FROM {{ ref('grid_proximity') }}
    UNION ALL
    SELECT 'release' AS model, mapped_buildings, buildings_within, share_within
    FROM {{ ref('release_proximity') }}
)
SELECT *
FROM summaries
WHERE mapped_buildings IS NULL OR mapped_buildings <= 0
   OR buildings_within IS NULL OR buildings_within < 0
   OR buildings_within > mapped_buildings
   OR share_within IS NULL OR share_within < 0 OR share_within > 1
   OR ABS(share_within - CAST(buildings_within AS DOUBLE) / mapped_buildings) > 1e-12
