package MovieRecommenderMapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ListCreator {
    static HashMap<String,Double> sentiments = new HashMap<>();
    static void main(String... args) throws Exception {
        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();
        Path input1 = new Path("output2/part-r-00000");
        Path input2 = new Path("output3/part-r-00000");
        Path output = new Path("output4");
        Job j = new Job(c, "ListCreator");
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        j.setJarByClass(MovieRecommender.class);
        MultipleInputs.addInputPath(j, input1, TextInputFormat.class, MapForSimilarityLoader.class);
        MultipleInputs.addInputPath(j, input2, TextInputFormat.class, MapForSentimentLoader.class);
        j.setReducerClass(ReduceForListCreator.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(true);
    }

    private static class MapForSimilarityLoader extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable key, Text similarities, Context con) throws
                IOException, InterruptedException {
            String[] similarity = similarities.toString().split("\t");
            if (similarity.length == 3)
                similarity = new String[]{similarity[0]+similarity[1], similarity[2]};
            String[] movies = similarity[0].split(",");
            con.write(new Text(movies[0]), new Text(movies[1] + ',' + similarity[1]));
            con.write(new Text(movies[1]), new Text(movies[0] + ',' + similarity[1]));
        }
    }

    private static class MapForSentimentLoader extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable key, Text movieSentiment, Context con) throws
                IOException, InterruptedException {
            String[] movieSentiments = movieSentiment.toString().split("\t");
            ListCreator.sentiments.put(movieSentiments[0],Double.parseDouble(movieSentiments[1]));
        }
    }

    private static class ReduceForListCreator extends Reducer<Text,
            Text, Text, Text> {
        public void reduce(Text movie, Iterable<Text> similarities, Context
                con) throws IOException, InterruptedException {
            Map<String, String> map = new HashMap<>();
            for (Text similarity : similarities) {
                String sim = similarity.toString();
                String movieId = sim.substring(0, sim.indexOf(','));
                Double movieSimilarity = Double.parseDouble(sim.substring(1 + sim.indexOf(',')));
                Double sentiment = ListCreator.sentiments.get(movieId);
                if (sentiment==null)sentiment=0d;
                Double score = movieSimilarity+0.1*sentiment;
                map.put(movieId, ""+score);
            }
            List<Map.Entry<String, String>> orderedList = map.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .limit(MovieRecommender.k)
                    .collect(Collectors.toList());
            StringBuilder orderOfRecommendation = new StringBuilder("");
            orderedList.forEach(mov -> orderOfRecommendation.append(',').append(mov.getKey()));
            con.write(movie, new Text(String.valueOf(orderOfRecommendation).substring(1)));
        }
    }
}
