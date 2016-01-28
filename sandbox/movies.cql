USE mykeyspace;
CREATE TABLE movies (row_id text PRIMARY KEY, name text, year text, rate text, duration text);
COPY movies FROM 'movies_data.csv';
-- WITH DELIMITER='\t';
--WITH HEADER=true;
SELECT * FROM movies;
