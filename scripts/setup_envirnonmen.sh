#!/bin/bash


mkdir temp_downloads
cd temp_downloads

# Install needed software
if [[ $OSTYPE == darwin* ]]; then
    # Install and update anaconda 
    wget https://repo.continuum.io/archive/Anaconda3-4.1.1-MacOSX-x86_64.sh
    bash Anaconda 3-4.1.1-MacOSX-x86_64.sh
    conda update conda
    conda update anaconda

elif [[ $OSTYPE == linux* ]]; then
    # Install and update anaconda
    wget https://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh
    bash Anaconda3-4.1.1-Linux-x86_64.sh 
    conda update conda
    conda update anaconda
fi

# Install MRjob
conda install pip
pip install mrjob







# Clean up folder/downloads
cd ..
rm -rf temp_downloads


