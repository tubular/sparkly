CREATE DATABASE if not exists sparkle_test;
CREATE TABLE sparkle_test.test_writer (
  uid varchar(30),
  title varchar(40),
  views int,
  primary key (uid)
);
INSERT INTO sparkle_test.test_writer (uid, title, views) VALUES ('1111', '1111', 999);
