
USE mykeyspace;
DROP TABLE task1_part2_group2_3;
CREATE TABLE task1_part2_group2_3 (
  origin text,
  dest text,
  carrier text,
  average float,
  sum float,
  count int,
  PRIMARY KEY ((origin, dest), average, carrier)
);
COPY task1_part2_group2_3 (origin, dest, carrier, average, sum, count) FROM 'task1_part2_group2_3.csv' WITH DELIMITER='\t';
-- AND NUMPROCESSES=1;
--WITH HEADER=true;
SELECT * FROM task1_part2_group2_3;
