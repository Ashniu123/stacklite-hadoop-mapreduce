import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTagsQuestionMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String record = values.toString();
        String[] parts = record.split(",");

        context.write(new Text(parts[0]), new Text(parts[1]));
    }
}
