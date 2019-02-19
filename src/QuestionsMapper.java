import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class QuestionsMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String record = values.toString();
        String[] parts = record.split(",");
        context.write(new Text(parts[0]), new Text("score\t" + parts[4]));
        context.write(new Text(parts[0]), new Text("owner\t" + parts[5]));
        context.write(new Text(parts[0]), new Text("answers\t" + parts[6]));
        context.write(new Text(parts[0]), new Text("creation_date\t" + parts[1]));
        context.write(new Text(parts[0]), new Text("closed_date\t" + parts[2]));
    }
}
