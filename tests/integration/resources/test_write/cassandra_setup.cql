CREATE KEYSPACE sparkle_test WITH replication =
{'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE sparkle_test.test_writer (
    video_uid text,
    account_uid text,
    title text,
    description text,
    views bigint,
    published text,
    PRIMARY KEY (video_uid)
);
