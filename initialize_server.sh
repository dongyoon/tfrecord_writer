#! /bin/bash

mkdir data
cd data
mkdir raw
mkdir campaign
mkdir tfrecords

sudo yum install python-pip python-dev python-virtualenv
pip install --upgrade pip