import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StackliteMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String record = values.toString();
        String[] parts = record.split("\t");

        context.write(new Text(parts[0]), new Text("tag\t" + parts[1]));
    }
}
