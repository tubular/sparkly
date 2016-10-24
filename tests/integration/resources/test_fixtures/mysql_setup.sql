CREATE DATABASE if not exists sparkle_test;

CREATE TABLE sparkle_test.test (
  id int,
  name varchar(30),
  surname varchar(40),
  age int,
  primary key (id)
);
INSERT INTO sparkle_test.test (id, name, surname, age) VALUES (1, 'john', 'sk', 111);
