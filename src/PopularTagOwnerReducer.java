import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PopularTagOwnerReducer extends Reducer<Text, Text, Text, Text> {
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

        HashMap<String, HashMap<String, Integer>> sortedMap = new HashMap<>();
        map.entrySet().stream()
                .forEach(stringHashMapEntry -> {
                    sortedMap.put(stringHashMapEntry.getKey(), stringHashMapEntry.getValue().entrySet().stream()
                            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                    (e1, e2) -> e1, LinkedHashMap::new)));
                });

        for (Map.Entry<String, HashMap<String, Integer>> entry : sortedMap.entrySet()) {
            if (entry.getKey().equals("null")) continue;
            context.write(new Text(entry.getKey()),
                    new Text(entry.getValue().entrySet().stream()
                            .limit(5)
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, o2) -> o, HashMap::new))
                            .toString()));
//            context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
    }
}
