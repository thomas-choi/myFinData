#!/bin/bash

CMDPATH=`dirname "$0"`
ProjDIR="$(dirname "$CMDPATH")"

cd $ProjDIR
mycmd="$CMDPATH/apply_gmodel.sh"
echo "Excute $mycmd @ `pwd`"

docker run -it --rm --ipc=host --ulimit memlock=-1 --ulimit stack=67108864 --shm-size=2g -u 1000:1000 \
    -v `pwd`:`pwd` -w `pwd` -v /etc/timezone:/etc/timezone -v /etc/localtime:/etc/localtime -v /etc/passwd:/etc/passwd \
    -v /etc/group:/etc/group -v /tmp:/tmp thomaschoi/stk-pred:v9.1-tf2-py3 /usr/bin/bash $mycmd

