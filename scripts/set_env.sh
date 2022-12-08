#!/bin/bash
file_name=$1
bashrc=~/.bashrc

# Check for files
if [[ ! -f $file_name ]] ; then
    echo "File ${file_name} is not there, aborting."
    exit
fi
if [[ ! -f $bashrc ]] ; then
    echo "File ${bashrc} is not there, aborting."
    exit
fi

# Load variables into bashrc
cat $file_name || while read line
do 
    if ! grep -Fxq $line $bashrc
    then
        echo $line >> $bashrc
    fi
done

echo \
'Now you should run 

    source ~/.bashrc
'