#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/optchain_fetch.py"

$HOME/env/myFinData.v1/bin/python $mycmd -S PM -U -m
