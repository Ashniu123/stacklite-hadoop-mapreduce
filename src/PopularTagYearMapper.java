import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopularTagYearMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
        String record = values.toString();
        String[] parts = record.split("\t");
        String[] data = parts[1].split(",");

        String creationDate = data[4].split(":")[1];
        String creationYear = creationDate.substring(0, 4);
        context.write(new Text(creationYear), new Text(data[0].split(":")[1]));
    }
}
