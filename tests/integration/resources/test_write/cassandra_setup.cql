DROP KEYSPACE IF EXISTS sparkly_test;

CREATE KEYSPACE sparkly_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE sparkly_test.test_writer (
    uid text,
    title text,
    views bigint,
    PRIMARY KEY (uid)
);
