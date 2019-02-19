# Hadoop MapReduce on Stacklite dataset

## Pre-requisites
1. Hadoop v2.7+ installed (I am using v3.2)
2. All Hadoop related environment variables and `JAVA_HOME` should be accessible to program

## Steps:
1. Download dataset from [here](https://www.kaggle.com/stackoverflow/stacklite/) and copy it into `input` folder
2. The current program expects dataset in `.txt` form so change extension from `.csv` to `.txt` and remove first line (column headings)
3. Open folder using your favorite IDE and choose `Maven` as type of project (You know what to do)
4. Run `src/Stacklite.java`
5. Check the output in the `output` folder

**Note**: The dataset is pretty big (~1GB each file) so I have added a `input/script.py` to generate a sample dataset (100,000 lines from each file) in same folder. Change the filenames in `Stacklite.java` file accordingly.

## What do I find out?
1. Most popular tag
2. Most tagged question
3. Tags with most answers
4. Popular tag by year
5. Top 5 Popular tag by owner

##  Additional: MapReduce in Python using Hadoop's streaming API
This assumes that the main program has been run successfully at least once since it uses one of its output files as a input.
How to run:
1. Change directory to `python` folder
2. Run `run_popular_year.sh` to find out popular tag by year
3. Run `run_rust.sh` to find popularity of year over the years and plot a graph of it