#!/bin/bash
## pWordCount.sh
## Author: James G. Shanahan
## Usage: pWordCount.sh m wordlist testFile.txt
## Input:
##       m = number of processes (maps), e.g., 4
##       wordlist = a space-separated list of words in quotes, e.g., "the and of" if set to "*", then all words are used
##       inputFile = a text input file
##
## Instructions: Read this script and its comments closely.
##               Do your best to understand the purpose of each command,
##               and focus on how arguments are supplied to mapper.py/reducer.py,
##               as this will determine how the python scripts take input.
##               When you are comfortable with the unix code below,
##               answer the questions on the LMS for HW1 about the starter code.


usage()
{
    echo ERROR: No arguments supplied
    echo
    echo To run use
    echo "     pWordCount.sh m wordlist inputFile"
    echo Input:
    echo "      number of processes/maps, EG, 4"
    echo "      wordlist = a space-separated list of words in quotes, e.g., 'the and of'" 
    echo "      inputFile = a text input file"
}

if [ $# -eq 0 ] # I removed a hash after the $ sign
  then
    usage  
    exit 1
fi
    
## collect user input
m=$1 ## the number of parallel processes (maps) to run

wordlist=$2 ## if set to "*", then all words are used

## a text file 
data=$3
    
## Clean up remaining chunks from a previous run
rm -f $data.chunk.*

## 'wc' determines the number of lines in the data
## 'perl -pe' regex strips the piped wc output to a number
linesindata=`wc -l $data | perl -pe 's/^.*?(\d+).*?$/$1/'`

## determine the lines per chunk for the desired number of processes
linesinchunk=`echo "$linesindata/$m+1" | bc`

## split the original file into chunks by line
split -l $linesinchunk $data $data.chunk.

## assign python mappers (mapper.py) to the chunks of data
## and emit their output to temporary files
for datachunk in $data.chunk.*; do
    ## feed word list to the python mapper here and redirect STDOUT to a temporary file on disk
    ####
    ####
    ./mapper.py  "$wordlist" <$datachunk > $datachunk.counts &
    ####
    ####
done
## wait for the mappers to finish their work
wait

###----------------------------------------------------------------------------------------
# Sorting magic
    
hash_to_bucket ()
{
# Takes as input a string and n_buckets and assigns the string to one of the buckets.
# This function is the bottle neck and takes forever to run
string=$1
n_buckets=$2

# Convert to checksum then take the mod of it
echo -n $string | cksum | cut -f1 -d" " | awk -v n="$n_buckets" '{print $1"%"n}' | bc
}

sorted_file_prefix=sorted_pwordcount

rm -f $sorted_file_prefix* *output

# For each file with counts, create coallated files
for file in $data.chunk.*.counts
    do
        cat $file | while read line
            do
                key=$(echo $line | cut -f1 -d" ")
                hash_key=$(hash_to_bucket $key $m)
                echo $line >> $sorted_file_prefix.$hash_key
            done
    done

# Sort these files
for file in $sorted_file_prefix*
    do
        sort $file -o $file
    done
#
###----------------------------------------------------------------------------------------

    
## 'ls' makes a list of the temporary count files
## 'perl -pe' regex replaces line breaks with spaces
countfiles=`\ls $sorted_file_prefix* | perl -pe 's/\n/ /'` # replace file reference to one from the sort.

## feed the list of countfiles to the python reducer and redirect STDOUT to disk
####
####
for file in $sorted_file_prefix*
    do
        <$file ./reducer.py >> $data.output
    done
    
sort $data.output -o $data.output
####
####

## clean up the data chunks and temporary count files
rm $data.chunk.*