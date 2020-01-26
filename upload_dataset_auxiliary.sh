#!/usr/bin/env bash

cd ~
LINK=$(cat dataset_address.txt) # already there
mkdir "movie_db" && cd movie_db/
echo "Retrieving: $LINK" && wget $LINK


unzip movies.zip
rm movies.zip

hadoop fs -mkdir "movie_db"

for csv_file in ./*;
  do hadoop fs -copyFromLocal "$csv_file" "movie_db/$csv_file"
  echo "$csv_file copied to movie_db/$csv_file";
done

echo 'Dataset uploaded successfully'

exit
