#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR

echo `pwd`

mycmd="$projDIR/eod_usrate.py"

$HOME/env/myFinData/bin/python $mycmd
