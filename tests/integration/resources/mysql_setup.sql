CREATE DATABASE if not exists sparkle_test;
CREATE TABLE sparkle_test.test_writer (
  video_uid varchar(30),
  account_uid varchar(40),
  title varchar(40),
  description varchar(40),
  views int,
  published varchar(40),
  primary key (video_uid)
);
INSERT INTO sparkle_test.test_writer (video_uid, account_uid, title, description, views, published) VALUES ('1111', '1111', 'john', 'site of john', 999, '201601212');

CREATE TABLE sparkle_test.test (
  id int,
  name varchar(30),
  surname varchar(40),
  age int,
  primary key (id)
);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (1, 'john', 'sk', 111);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (2, 'john', 'po', 222);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (3, 'john', 'ku', 333);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (4, 'john', 'bo', 444);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (5, 'john', 'st', 999);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (6, 'john', 'mo', 123);
