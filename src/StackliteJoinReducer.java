import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StackliteJoinReducer extends Reducer<Text, Text, Text, Text> {
    String tag, score, owner, answers, creationDate, closedDate;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            switch (parts[0]) {
                case "tag":
                    tag = parts[1];
                    break;
                case "score":
                    score = parts[1];
                    break;
                case "owner":
                    owner = parts[1].equals("NA") ? null : parts[1];
                    break;
                case "answers":
                    answers = parts[1];
                    break;
                case "creation_date":
                    creationDate = parts[1];
                    break;
                case "closed_date":
                    closedDate = parts[1];
                    break;
            }
        }
        context.write(key, new Text("tag:" + tag + ",score:" + score + ",owner:" + owner + ",answers:" + answers
                + ",creation_date:" + creationDate + ",closed_date:" + closedDate));
    }
}
