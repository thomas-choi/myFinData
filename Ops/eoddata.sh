#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/eoddata_fetch.py"

$HOME/env/myFinData/bin/python $mycmd
