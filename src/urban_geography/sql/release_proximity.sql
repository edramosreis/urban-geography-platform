-- Roll up counts, then divide. Averaging cell shares would weight small cells too much.
WITH counts AS (
    SELECT
        source_release,
        area_id,
        source_run_sha256,
        SUM(mapped_buildings) AS mapped_buildings,
        SUM(buildings_within) AS buildings_within
    FROM {grids}
    GROUP BY source_release, area_id, source_run_sha256
)
SELECT
    *,
    CAST(buildings_within AS DOUBLE) / mapped_buildings AS share_within
FROM counts
ORDER BY source_release, area_id
