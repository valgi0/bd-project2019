#!/usr/bin/env bash

./gradlew mapReduceJarJob1

scp ./build/libs/*-mr1.jar lvalgimigli@isi-vclust9.csr.unibo.it:/home/lvalgimigli/bd-project2019-master/mapreduce