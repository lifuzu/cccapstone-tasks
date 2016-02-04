USE mykeyspace;
--DROP TABLE task1_part2_group3_2;
CREATE TABLE task1_part2_group3_2 (
  origin text,
  through text,
  dest text,
  delay float,
  first_date text,
  PRIMARY KEY (origin, through, dest, first_date)
);
