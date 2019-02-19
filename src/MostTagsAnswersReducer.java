import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MostTagsAnswersReducer extends Reducer<Text, Text, Text, Text> {
    private HashMap<String, Integer> map = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context) {
        int count = 0;
        for (Text value : values) {
            int num;
            try {
                num = Integer.parseInt(value.toString());
            } catch (NumberFormatException nfe) {
                num = 0;
            }
            count += num;
        }
        map.put(key.toString(), count);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Sort by value
        HashMap<String, Integer> sortedMap = map.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (e1, e2) -> e1, LinkedHashMap::new));

        for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
            context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
    }
}
