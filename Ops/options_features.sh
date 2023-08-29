#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/options_features.py"

$HOME/env/myFinData/bin/python $mycmd -U
