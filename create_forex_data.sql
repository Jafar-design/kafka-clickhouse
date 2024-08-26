CREATE TABLE forex_data_flat (
    ticker String,
    queryCount UInt32,
    resultsCount UInt32,
    adjusted Bool,
    v UInt64,
    vw Float32,
    o Float32,
    c Float32,
    h Float32,
    l Float32,
    t DateTime64(3),
    n UInt64,
    status String,
    request_id String,
    count UInt32
) ENGINE = MergeTree()
ORDER BY t;
