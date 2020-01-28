#!/bin/bash
# uploads a specified file to transfer.sh
# stores the link to the file followed by the timestamp to which it is available

EXPIRY_DATE=$(date -d "+14 days")

upload() {
  curl --upload-file "$0" https://transfer.sh/script  >> ../files_uploaded.txt
}

if upload ; then
  echo "Expiry date: $EXPIRY_DATE" >> ../files_uploaded.txt
  echo "----------------------------------------" >> ../files_uploaded.txt
  echo "" >> ../files_uploaded.txt

  echo "Your link expires at: $EXPIRY_DATE"
else
  echo "Try again, something went wrong"
fi
