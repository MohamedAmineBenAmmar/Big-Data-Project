-- Accept filename as a parameter
%declare FILE_PATH '$file_path'

-- Extract the filename from the file path
%declare FILE_NAME `basename $FILE_PATH`

-- Load the dataset
A = LOAD '$FILE_PATH' USING TextLoader() AS (line:chararray);

-- Tokenize the words
B = FOREACH A GENERATE FLATTEN(TOKENIZE(line, '\\W+')) AS word;

-- Group by each word
C = GROUP B BY word;

-- Count the occurrences
D = FOREACH C GENERATE group AS word, COUNT(B) AS count;

-- Add the filename to each record
E = FOREACH D GENERATE word, count, '$FILE_NAME' AS filename;

-- Store the result
STORE E INTO '/out/pigwordcount' USING PigStorage(',');

