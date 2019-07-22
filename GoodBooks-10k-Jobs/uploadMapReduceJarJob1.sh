#!/usr/bin/env bash

./gradlew mapReduceJarJob1

scp ./build/libs/GoodBooks-10k-Jobs-2.1.2-mr1.jar lvalgimigli@isi-vclust9.csr.unibo.it:/home/lvalgimigli/exam/mapreduce