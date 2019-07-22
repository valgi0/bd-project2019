#!/usr/bin/env bash

./gradlew mapReduceJarJob2

scp ./build/libs/*-mr2.jar lvalgimigli@isi-vclust9.csr.unibo.it:/home/lvalgimigli/exam/mapreduce