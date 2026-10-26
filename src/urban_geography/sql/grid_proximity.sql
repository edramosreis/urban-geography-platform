-- Grain: one source release / study area / occupied 250 m grid cell.
-- Source hashes identify the single verified processing run in each partition.
WITH counts AS (
    SELECT
        source_release,
        area_id,
        source_run_sha256,
        grid_id,
        COUNT(*) AS mapped_buildings,
        SUM(CASE WHEN within_threshold THEN 1 ELSE 0 END) AS buildings_within
    FROM {facts}
    GROUP BY source_release, area_id, source_run_sha256, grid_id
)
SELECT
    *,
    CAST(buildings_within AS DOUBLE) / mapped_buildings AS share_within
FROM counts
ORDER BY source_release, area_id, grid_id
