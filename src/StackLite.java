import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class StackLite {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        /*
         * Popular Tag job
         * To find the most popular tag
         */
        String inputTagsPath = "./input/question_tags.txt";
        String popularTagJobOutputFilePath = "./output/most_popular_tag";

        File f = new File(popularTagJobOutputFilePath);
        FileUtils.deleteQuietly(f);

        Job popularTagJob = Job.getInstance(conf, "STACKLITE_POPULAR_TAG");
        popularTagJob.setJarByClass(StackLite.class);
        popularTagJob.setMapperClass(PopularTagMapper.class);
        popularTagJob.setReducerClass(PopularTagReducer.class);
        popularTagJob.setOutputKeyClass(Text.class);
        popularTagJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(popularTagJob, new Path(inputTagsPath));
        FileOutputFormat.setOutputPath(popularTagJob, new Path(popularTagJobOutputFilePath));

        /*
         * questions tags job
         * To find the most tagged question
         */
        String mostTaggedQuestionJobOutputFilePath = "./output/most_tagged_question";
        f = new File(mostTaggedQuestionJobOutputFilePath);
        FileUtils.deleteQuietly(f);

        Job mostTaggedQuestionJob = Job.getInstance(conf, "STACKLITE_TAG_QUESTION");
        mostTaggedQuestionJob.setJarByClass(StackLite.class);
        mostTaggedQuestionJob.setMapperClass(MostTagsQuestionMapper.class);
        mostTaggedQuestionJob.setReducerClass(MostTagsQuestionReducer.class);
        mostTaggedQuestionJob.setOutputKeyClass(Text.class);
        mostTaggedQuestionJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(mostTaggedQuestionJob, new Path(inputTagsPath));
        FileOutputFormat.setOutputPath(mostTaggedQuestionJob, new Path(mostTaggedQuestionJobOutputFilePath));

        /*
         * Join Job
         * To join the 2 datasets
         */
        String inputQuestionsPath = "./input/questions.txt";
        String joinJobOutputFilePath = "./output/join_job";

        f = new File(joinJobOutputFilePath);
        FileUtils.deleteQuietly(f);

        Job joinJob = Job.getInstance(conf, "STACKLITE_JOIN");
        joinJob.setJarByClass(StackLite.class);
        joinJob.setReducerClass(StackliteJoinReducer.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(joinJob, new Path(inputTagsPath), TextInputFormat.class, TagsMapper.class);
        MultipleInputs.addInputPath(joinJob, new Path(inputQuestionsPath), TextInputFormat.class, QuestionsMapper.class);
        FileOutputFormat.setOutputPath(joinJob, new Path(joinJobOutputFilePath));

        joinJob.setNumReduceTasks(3);

        /*
         * Tags with most answers job
         * To generate useful output from joinJob
         */
        String tagsAnswersJobOutputFilePath = "./output/tags_most_answers";

        f = new File(tagsAnswersJobOutputFilePath);
        FileUtils.deleteQuietly(f);

        Job tagsAnswersJob = Job.getInstance(conf, "STACKLITE_TAGS_MOST_ANSWERS");
        tagsAnswersJob.setJarByClass(StackLite.class);
        tagsAnswersJob.setMapperClass(MostTagsAnswersMapper.class);
        tagsAnswersJob.setReducerClass(MostTagsAnswersReducer.class);
        tagsAnswersJob.setOutputKeyClass(Text.class);
        tagsAnswersJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(tagsAnswersJob, new Path(joinJobOutputFilePath + "/final"));
        FileOutputFormat.setOutputPath(tagsAnswersJob, new Path(tagsAnswersJobOutputFilePath));

        /*
         * Popular tag by year
         * To generate useful output from joinJob
         */
        String popularTagYearJobOutputFilePath = "./output/popular_tag_year";

        f = new File(popularTagYearJobOutputFilePath);
        FileUtils.deleteQuietly(f);

        Job popularTagYearJob = Job.getInstance(conf, "STACKLITE_POPULAR_TAG_YEAR");
        popularTagYearJob.setJarByClass(StackLite.class);
        popularTagYearJob.setMapperClass(PopularTagYearMapper.class);
        popularTagYearJob.setReducerClass(PopularTagYearReducer.class);
        popularTagYearJob.setOutputKeyClass(Text.class);
        popularTagYearJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(popularTagYearJob, new Path(joinJobOutputFilePath + "/final"));
        FileOutputFormat.setOutputPath(popularTagYearJob, new Path(popularTagYearJobOutputFilePath));

        popularTagYearJob.setNumReduceTasks(2);

        /*
         * Popular tag by owner
         * To generate useful output from joinJob
         */
        String popularTagOwnerJobOutputFilePath = "./output/popular_tag_owner";

        f = new File(popularTagOwnerJobOutputFilePath);
        FileUtils.deleteQuietly(f);

        Job popularTagOwnerJob = Job.getInstance(conf, "STACKLITE_POPULAR_TAG_OWNER");
        popularTagOwnerJob.setJarByClass(StackLite.class);
        popularTagOwnerJob.setMapperClass(PopularTagOwnerMapper.class);
        popularTagOwnerJob.setReducerClass(PopularTagOwnerReducer.class);
        popularTagOwnerJob.setOutputKeyClass(Text.class);
        popularTagOwnerJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(popularTagOwnerJob, new Path(joinJobOutputFilePath + "/final"));
        FileOutputFormat.setOutputPath(popularTagOwnerJob, new Path(popularTagOwnerJobOutputFilePath));

        popularTagOwnerJob.setNumReduceTasks(2);

        /*
         * Run unrelated jobs in parallel
         */
        popularTagJob.submit();
        mostTaggedQuestionJob.submit();

        /*
         * Run jobs in parallel after join operation
         */
        if (joinJob.waitForCompletion(true)) {
            Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", "hadoop fs -getmerge " + joinJobOutputFilePath + " " + joinJobOutputFilePath + "/final"});
            p.waitFor();
            System.out.println(p.exitValue());
            if (p.exitValue() == 0) {
                tagsAnswersJob.submit();
                if(popularTagYearJob.waitForCompletion(true)) {
                    Process p1 = Runtime.getRuntime().exec(new String[]{"bash", "-c", "hadoop fs -getmerge " + popularTagYearJobOutputFilePath + " " + popularTagYearJobOutputFilePath + "/final"});
                    p1.waitFor();
                }
                if (popularTagOwnerJob.waitForCompletion(true)) {
                    Process p2 = Runtime.getRuntime().exec(new String[]{"bash", "-c", "hadoop fs -getmerge " + popularTagOwnerJobOutputFilePath + " " + popularTagOwnerJobOutputFilePath + "/final"});
                    p2.waitFor();
                }
            }
        }
    }
}
