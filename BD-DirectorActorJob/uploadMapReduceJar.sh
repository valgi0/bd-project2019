#!/usr/bin/env bash

./gradlew mapReduceJar

scp ./build/libs/*-mr.jar lvalgimigli@isi-vclust9.csr.unibo.it:/