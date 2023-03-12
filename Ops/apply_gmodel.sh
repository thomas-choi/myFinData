#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR

mycmd="$projDIR/Apply_gmodel.py"

echo "Jobs $mycmd are running at `pwd`"
echo "================================="

echo "Using `which python`"
python --version
python $mycmd
