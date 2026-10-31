-- The run hash is supplied provenance, not proof of the snapshot's identity.
-- Live execution must first verify it against the writer/table receipt.
SELECT
    building_id,
    park_id,
    distance_m,
    within_threshold,
    grid_id,
    source_release,
    area_id,
    '{{ var("source_run_sha256") }}' AS source_run_sha256
FROM {{ source('lakehouse', 'building_proximity') }}
FOR VERSION AS OF {{ var('snapshot_id') }}
