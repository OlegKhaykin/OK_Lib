#!/usr/bin/ksh
for i in `seq 1 $3` ; do mysql -uok -pm OK -e "call prc_test_log($1, $2);" & ; done
