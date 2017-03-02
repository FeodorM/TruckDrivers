#!/usr/bin/env bash

rm -r result
/usr/local/spark-2.1.0-bin-hadoop2.7/bin/spark-submit \
    ~/code/IdeaProjects/TruckDrivers/target/scalasparktest.jar \
    ~/code/IdeaProjects/TruckDrivers/sourceData/geolocation.csv \
    ~/code/IdeaProjects/TruckDrivers/sourceData/trucks.csv \
    result
