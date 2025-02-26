#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/optchain_fetch.py"

$HOME/env/myFinData.v2/bin/python $mycmd -S PM -U -m
