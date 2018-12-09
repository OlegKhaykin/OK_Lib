#!/usr/bin/ksh
for i in `seq 1 100` ; do mysql -uok -pm OK -e "call prc_multiply($1);" & ; done
