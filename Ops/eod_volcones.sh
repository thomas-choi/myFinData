#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/eod_volcones.py"

$HOME/env/myFinData/bin/python $mycmd -t -S
