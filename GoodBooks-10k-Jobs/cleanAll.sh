#!/usr/bin/env bash

rm -rf ~/bd-project2019-master/dataset/output/*
hdfs dfs -rm -r bd-project2019-master/dataset/output/*
rm -f ~/bd-project2019-master/mapreduce/*