import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PopularTagYearReducer extends Reducer<Text, Text, Text, Text> {
    private HashMap<String, HashMap<String, Integer>> map = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context) {
        HashMap<String, Integer> tagCount = new HashMap<>();

        for (Text value : values) {
            if (tagCount.containsKey(value.toString())) {
                int curr = tagCount.get(value.toString());
                tagCount.put(value.toString(), curr + 1);
            } else {
                tagCount.put(value.toString(), 1);
            }
        }
        map.put(key.toString(), tagCount);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Replace Hashmap with max value
        HashMap<String, String> sortedMap = new HashMap<>();
        map.entrySet().forEach(stringHashMapEntry -> {
            Map.Entry m = stringHashMapEntry.getValue().entrySet().stream().max((e1, e2) -> e1.getValue() > e2.getValue() ? 1 : -1).get();
            sortedMap.put(stringHashMapEntry.getKey(), m.getKey() + " :- " + m.getValue());
        });

        for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        }
    }
}
