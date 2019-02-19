chmod +x year_mapper.py
chmod +x year_reducer.py

rm -rf ./output_year
rm -rf ./input
hadoop dfs -mkdir input
hadoop dfs -copyFromLocal ../output/join_job/final ./input/final

head -n1000 ../output/join_job/final | ./year_mapper.py | sort | ./year_reducer.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar \
-file ./year_mapper.py -mapper ./year_mapper.py \
-file ./year_reducer.py -reducer ./year_reducer.py \
-input ./input/final -output ./output_year
