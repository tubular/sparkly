DROP KEYSPACE IF EXISTS sparkle_test;

CREATE KEYSPACE sparkle_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE sparkle_test.test_writer (
    uid text,
    title text,
    views bigint,
    PRIMARY KEY (uid)
);
