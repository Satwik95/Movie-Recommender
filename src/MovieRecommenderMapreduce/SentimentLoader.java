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

class SentimentLoader {
    static void main(String... args) throws Exception {
        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("lexicon.csv");
        Path output = new Path("output_pseudo");
        Job j = new Job(c, "SentimentLoader");
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        j.setJarByClass(MovieRecommender.class);
        j.setMapperClass(MapForSentimentLoader.class);
        j.setReducerClass(ReduceForSentimentLoader.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(true);
    }

    private static class MapForSentimentLoader extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable line, Text sentiment, Context con) throws
                IOException, InterruptedException {
            String s[] = sentiment.toString().split("\t");
            MovieRecommender.lexicon.put(s[0], Double.parseDouble(s[1]));
        }
    }

    private static class ReduceForSentimentLoader extends Reducer<Text,
            Text, Text, Text> {
        public void reduce(Text moviePair, Iterable<Text> ratingPairs, Context
                con) throws IOException, InterruptedException {

        }
    }
}
