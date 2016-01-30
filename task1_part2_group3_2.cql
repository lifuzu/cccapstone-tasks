
USE mykeyspace;
DROP TABLE task1_part2_group3_2;
CREATE TABLE task1_part2_group3_2 (
  rank int,
  origin text,
  through text,
  dest text,
  delay float,
  first_date text,
  PRIMARY KEY (origin, through, dest, first_date)
);
COPY task1_part2_group3_2 (rank, origin, through, dest, delay, first_date) FROM 'task1_part2_group3_2.csv' WITH DELIMITER='\t';
-- AND NUMPROCESSES=1;
--WITH HEADER=true;
SELECT * FROM task1_part2_group3_2;
