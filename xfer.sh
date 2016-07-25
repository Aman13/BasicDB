#!/bin/sh
if [ $# -ne 1 ]
then
  echo "Must specify IP address"
  exit 1
fi

KEY_FILE="/Users/generiglesias/Documents/cmpt474/AmazonEC2Key/aws.pem"

scp -i $KEY_FILE backend.py duplicator.py frontend.py front_ops.py SendMsg.py create_ops.py delete_ops.py retrieve_ops.py update_ops.py ubuntu@$1:/home/ubuntu
