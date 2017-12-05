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

class SentimentAnalyzer {
    static void main(String... args) throws Exception {
        SentimentLoader.main(args);
        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("reviews.csv"); //scraped data
        Path output = new Path("output3");
        Job j = new Job(c, "SentimentAnalyzer");
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        j.setJarByClass(MovieRecommender.class);
        j.setMapperClass(MapForSentimentAnalyzer.class);
        j.setReducerClass(ReduceForSentimentAnalyzer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(true);
    }

    private static class MapForSentimentAnalyzer extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable user, Text review, Context con) throws
                IOException, InterruptedException {
            //converting to String so that we can call String methods
            String rev = review.toString(); 
            //review in the form of (1, It's a one time watch.........4 stars)
            int index = rev.indexOf(',');
            String movieId = rev.substring(0, index);
            String r = rev.substring(index + 1);
            //remove non chracters and non whitespaces from the reviews and then split by spaces between the words
            String[] words = r.replaceAll("[^\\p{L}\\p{Z}-]", "").toLowerCase().split(" ");
            double sentiment = 0;
            //calculate the sentiment from the lexicon strored in the form of hash maps
            for (String word : words) {
                Double s = MovieRecommender.lexicon.get(word);
                if (s == null) s = 0d;
                /*summation of positive and negative words, by doing this we are 
                  treating the array of reviews as one single review for the movie and 
                  accomodating the reveiwers who strongly feel a certain way for the movie*/
                sentiment += s; 
            }
    		 
            sentiment /= words.length; 
            con.write(new Text(movieId), new Text("" + sentiment));
        }
    }

    private static class ReduceForSentimentAnalyzer extends Reducer<Text,
            Text, Text, Text> {
        public void reduce(Text movie, Iterable<Text> sentiments, Context
                con) throws IOException, InterruptedException {
            double sentiment = 0;
            int k = 0;
            for (Text sent : sentiments) {
                sentiment += Double.parseDouble(sent.toString());
                k++;
            }
            //to average the sentiment scores
            sentiment = sentiment / k;
            //output in t form: movie_id sentiment_value
            con.write(movie, new Text("" + sentiment));
        }
    }
}
