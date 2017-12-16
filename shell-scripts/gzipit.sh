#!/bin/sh 
while read dummy filename ; do
        echo "Reading $filename"
        hadoop fs -copyToLocal $filename .
        base=`basename $filename`
        gzip $base
        hadoop fs -copyFromLocal "$base.gz" "/user/xhermand/project/archives/$base.gz"
done
