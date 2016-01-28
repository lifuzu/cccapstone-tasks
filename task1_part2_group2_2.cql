
USE mykeyspace;
DROP TABLE task1_part2_group2_2;
CREATE TABLE task1_part2_group2_2 (
  origin text,
  dest text,
  average float,
  sum float,
  count int,
  PRIMARY KEY (origin, average, dest)
);
COPY task1_part2_group2_2 (origin, dest, average, sum, count) FROM 'task1_part2_group2_2.csv' WITH DELIMITER='\t';
-- AND NUMPROCESSES=1;
--WITH HEADER=true;
SELECT * FROM task1_part2_group2_2;
