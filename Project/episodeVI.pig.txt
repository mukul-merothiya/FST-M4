inputDialouges = LOAD 'hdfs://localhost:9000/user/root/inputs/EpisodeVI_dialouges.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
ranked = RANK inputDialouges;
OnlyDialouges = FILTER ranked by (RANK_inputDialouges > 2);
groupbyName = GROUP OnlyDialouges BY name;
names = FOREACH 'groupbyName' GENERATE $0 as name, COUNT($1) AS no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;
STORE namesOrdered INTO 'hdfs://localhost:9000/user/root/outputs/episodeVIOutput' USING PigStorage('\t');