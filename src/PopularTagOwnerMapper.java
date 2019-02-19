import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopularTagOwnerMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String record = values.toString();
        String[] parts = record.split("\t");
        String[] data = parts[1].split(",");

        context.write(new Text(data[2].split(":")[1]), new Text(data[0].split(":")[1]));
    }
}
