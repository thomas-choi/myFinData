#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/eoddata_ext_fetch.py"
echo $mycmd

$HOME/env/myFinData/bin/python $mycmd 
