CREATE TABLE IF NOT EXISTS "biggraphite"."datapoints_0" (
    metric uuid,
    time_start_ms bigint,
    offset smallint,
    value double,
    PRIMARY KEY ((metric, time_start_ms), offset))
    WITH CLUSTERING ORDER BY (offset DESC)
    AND default_time_to_live = 31536900
    AND compaction = {    'class': 'DateTieredCompactionStrategy',    'timestamp_resolution': 'MICROSECONDS',        'base_time_seconds': '87300',    'max_window_size_seconds': 172800000  };