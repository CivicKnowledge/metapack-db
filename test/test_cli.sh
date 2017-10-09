#!/usr/bin/env bash


mt db load \
    http://library.metatab.org.s3.amazonaws.com/example.com-example_data_package-2017-us-1.xlsx \
    sqlite:////tmp/test.db \
    metapack+http://library.metatab.org/example.com-simple_example-2017-us-2