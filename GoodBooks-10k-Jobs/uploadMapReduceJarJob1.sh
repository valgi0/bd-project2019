#!/usr/bin/env bash

./gradlew mapReduceJarJob1

scp ./build/libs/GoodBooks-10k-Jobs-2.1.2-spark.jar lvalgimigli@isi-vclust9.csr.unibo.it:/home/lvalgimigli/bd-project2019-master/mapreduce