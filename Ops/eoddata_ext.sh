#!/bin/bash

projDIR=`dirname "$0"`

cd $projDIR 

echo `pwd`

mycmd="$projDIR/eoddata_ext_fetch.py -C ../Prod_config/Stk_eodfetch_DO.env"
echo $mycmd

$HOME/env/myFinData/bin/python $mycmd 
