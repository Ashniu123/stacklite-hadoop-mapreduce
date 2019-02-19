chmod +x rust_mapper.py
chmod +x rust_reducer.py
chmod +x plotter.py

rm -rf ./output_rust
rm -rf ./input
hadoop dfs -mkdir input
hadoop dfs -copyFromLocal ../output/join_job/final ./input/final

head -n1000 ../output/join_job/final | ./rust_mapper.py | sort | ./rust_reducer.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar \
-file ./rust_mapper.py -mapper ./rust_mapper.py \
-file ./rust_reducer.py -reducer ./rust_reducer.py \
-input ./input/final -output ./output_rust \
-numReduceTasks 2

hadoop fs -getmerge ./output_rust ./output_rust/final

./plotter.py
