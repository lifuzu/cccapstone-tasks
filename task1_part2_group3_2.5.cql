
USE mykeyspace;

COPY task1_part2_group3_2 (origin, through, dest, delay, first_date) FROM 'task1_part2_group3_2.5.csv' WITH DELIMITER='\t';
-- AND NUMPROCESSES=1;
--WITH HEADER=true;
--SELECT * FROM task1_part2_group3_2;
