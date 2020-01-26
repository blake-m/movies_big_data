#!/usr/bin/env bash

# Putting the resulting link to a file for later reuse
# curl --upload-file movies.zip https://transfer.sh/movies.zip > dataset_address.txt

# get variables
. .hadoop

# There are safer ways of course, but this is just a fun projcet
# on a completely not important VM with not important data, etc.
sshpass -p "$pass" ssh "$login@127.0.0.1" -p 2222 'bash -s' < upload_dataset_auxiliary.sh

echo 'Returned to local machine successfuly.'

exit