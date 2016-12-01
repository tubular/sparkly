DROP KEYSPACE IF EXISTS sparkly_test;

CREATE KEYSPACE sparkly_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE sparkly_test.test (
    uid text,
    created text,
    countries map<text, bigint>,
    PRIMARY KEY (uid, created)
);

INSERT INTO sparkly_test.test (uid, created, countries)
VALUES ('1', '1234567894', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkly_test.test (uid, created, countries)
VALUES ('2', '1234567893', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkly_test.test (uid, created, countries)
VALUES ('3', '1234567891', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});
