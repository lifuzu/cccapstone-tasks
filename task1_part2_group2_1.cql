
USE mykeyspace;
DROP TABLE task1_part2_group2_1;
CREATE TABLE task1_part2_group2_1 (
  origin text,
  carrier text,
  average float,
  sum float,
  count int,
  PRIMARY KEY (origin, average, carrier)
);
COPY task1_part2_group2_1 (origin, carrier, average, sum, count) FROM 'task1_part2_group2_1.csv' WITH DELIMITER='\t';
-- AND NUMPROCESSES=1;
--WITH HEADER=true;
SELECT * FROM task1_part2_group2_1;
