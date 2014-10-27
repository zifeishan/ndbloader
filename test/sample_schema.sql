-- DROP TABLE IF EXISTS test;
-- CREATE TABLE test(
--   id VARCHAR(255),
--   -- id CHAR(255),
--   -- id TEXT,
--   value int,
--   PRIMARY KEY(value)
-- ) engine=ndb;

DROP TABLE IF EXISTS test;
CREATE TABLE test(
  id bigint,
  gram text,
  count real) engine=ndb;