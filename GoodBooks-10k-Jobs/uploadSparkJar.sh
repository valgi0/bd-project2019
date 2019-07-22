#!/usr/bin/env bash

./gradlew sparkJar

scp ./build/libs/*-spark.jar lvalgimigli@isi-vclust9.csr.unibo.it:/home/lvalgimigli/exam/sparksql