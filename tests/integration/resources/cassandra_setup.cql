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

CREATE TABLE sparkle_test.test (
    uid text,
    created text,
    countries map<text, bigint>,
    PRIMARY KEY (uid, created)
);

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('1', '1234567899', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('2', '1234567898', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('3', '1234567897', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('4', '1234567896', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('5', '1234567895', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('6', '1234567894', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('7', '1234567893', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('8', '1234567892', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});

INSERT INTO sparkle_test.test (uid, created, countries)
VALUES ('9', '1234567891', {'AE': 13, 'BE': 1, 'BH': 3, 'CA': 1, 'DZ': 1, 'EG': 206});


