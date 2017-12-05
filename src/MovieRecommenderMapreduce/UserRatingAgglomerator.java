package MovieRecommenderMapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

class UserRatingAgglomerator {
    static void main(String... args) throws Exception {
        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("ratings.csv");
        Path output = new Path("output1");
        Job j = new Job(c, "UserRatingAgglomerator");
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        j.setJarByClass(MovieRecommender.class);
        j.setMapperClass(UserRatingAgglomerator.MapForUserRatingAgglomerator.class);
        j.setReducerClass(UserRatingAgglomerator.ReduceForUserRatingAgglomerator.class);
        j.setOutputKeyClass(LongWritable.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(true);
    }

    private static class MapForUserRatingAgglomerator extends Mapper<LongWritable, Text,
            LongWritable, Text> {
        public void map(LongWritable key, Text rating, Context con) throws
                IOException, InterruptedException {
            String txt = rating.toString();
            String[] lines = txt.split("\n");
            try {
                for (String line : lines) {
                    String[] words = line.split(",");
                    LongWritable outputKey = new LongWritable(Integer.parseInt(words[0]));
                    Text outputValue = new Text("" + words[1] + ' ' + words[2]);
                    con.write(outputKey, outputValue);

                }
            } catch (NumberFormatException e) {
                System.err.print("");
            }
        }
    }

    private static class ReduceForUserRatingAgglomerator extends Reducer<LongWritable,
            Text, LongWritable, Text> {
        public void reduce(LongWritable user, Iterable<Text> ratings, Context
                con) throws IOException, InterruptedException {
            StringBuilder list = new StringBuilder();
            for (Text value : ratings)
                list.append(',').append(value.toString());
            con.write(user, new Text(list.substring(1)));
        }
    }
}
