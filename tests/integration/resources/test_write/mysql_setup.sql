CREATE DATABASE if not exists sparkly_test;
CREATE TABLE sparkly_test.test_writer (
  uid varchar(30),
  title varchar(40),
  views int,
  primary key (uid)
);
INSERT INTO sparkly_test.test_writer (uid, title, views) VALUES ('1111', '1111', 999);
