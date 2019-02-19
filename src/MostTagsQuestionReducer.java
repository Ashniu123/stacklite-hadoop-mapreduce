import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MostTagsQuestionReducer extends Reducer<Text, Text, Text, Text> {
    private HashMap<String, ArrayList<String>> map = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context) {
        ArrayList<String> tags = new ArrayList<>();

        for (Text value : values) {
            tags.add((value.toString()));
        }

        map.put(key.toString(), tags);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Sort by value
        HashMap<String, ArrayList<String>> sortedMap = map.entrySet().stream()
                .sorted((stringArrayListEntry, t1) -> Integer.compareUnsigned(t1.getValue().size(), stringArrayListEntry.getValue().size()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        for (Map.Entry<String, ArrayList<String>> entry : sortedMap.entrySet()) {
            ArrayList<String> entryValue = entry.getValue();
            context.write(new Text(entry.getKey()), new Text(entryValue.toString() + ", " + entryValue.size()));
        }
    }
}
