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
