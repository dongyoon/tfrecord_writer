#! /bin/bash

YEAR=2017
MONTH=03
DATA_FOLDER_PATH="/home/ec2-user/data/raw/$YEAR/$MONTH"

for DAY in $(seq 21 22)
do
    d=$(printf %02d $DAY)
    for HOUR in $(seq 0 23)
    do
        h=$(printf %02d $HOUR)
        mkdir -p $DATA_FOLDER_PATH/$d/$h
        aws s3 cp s3://prod-buzzvil-firehose/buzzscreen/click/2017/03/$d/$h/ $DATA_FOLDER_PATH/$d/$h/ --recursive
        aws s3 cp s3://prod-buzzvil-firehose/buzzscreen/impression/2017/03/$d/$h/ $DATA_FOLDER_PATH/$d/$h/ --recursive
        python -u main.py --process_datetime="$YEAR$MONTH$d$h"
        rm -r $DATA_FOLDER_PATH/$d/$h/
    done
done