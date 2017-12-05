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
import java.util.ArrayList;

class SimilarityComputer {
    static void main(String... args) throws Exception {
        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("output1/part-r-00000");
        Path output = new Path("output2");
        Job j = new Job(c, "SimilarityComputer");
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        j.setJarByClass(MovieRecommender.class);
        j.setMapperClass(MapForSimilarityComputer.class);
        j.setReducerClass(ReduceForSimilarityComputer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(true);
    }

    private static class MapForSimilarityComputer extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable user, Text all_ratings, Context con) throws
                IOException, InterruptedException {
            ArrayList<MovieRating> movieRatings = new ArrayList<>();
            String txt = all_ratings.toString();
            String[] lines = txt.split("\n");
            for (String line : lines) {
                movieRatings.clear();
                String[] ratings = line.split(",");
                for (String singleRating : ratings) {
                    String movie = singleRating.substring(0, singleRating.indexOf(' '));
                    String rating = singleRating.substring(1 + singleRating.indexOf(' '));
                    movieRatings.add(new MovieRating(movie, rating));
                }
                addPair(con, movieRatings);
            }
        }

        private void addPair(Context con, ArrayList<MovieRating> movieRatings) throws
                IOException, InterruptedException {
            int n = movieRatings.size();
            for (int i = 0; i < n - 1; i++) {
                MovieRating rating = movieRatings.get(i);
                String movie1 = rating.getMovie();
                String rating1 = rating.getRating();
                for (int j = i + 1; j < n; j++) {
                    rating = movieRatings.get(j);
                    String movie2 = rating.getMovie();
                    String rating2 = rating.getRating();
                    if (movie1.compareTo(movie2)<0)
                        con.write(new Text(movie1 + ',' + movie2), new Text(rating1 + ',' + rating2));
                    else
                        con.write(new Text(movie2 + ',' + movie1), new Text(rating2 + ',' + rating1));
                }
            }
        }
    }

    private static class ReduceForSimilarityComputer extends Reducer<Text,
            Text, Text, Text> {
        public void reduce(Text moviePair, Iterable<Text> ratingPairs, Context
                con) throws IOException, InterruptedException {
            float a = 0, b = 0, numerator = 0;
            for (Text ratingPair : ratingPairs) {
                String rating = ratingPair.toString();
                float rating1 = Float.parseFloat(rating.substring(0, rating.indexOf(',')));
                float rating2 = Float.parseFloat(rating.substring(1 + rating.indexOf(',')));
                a += rating1 * rating1;
                b += rating2 * rating2;
                numerator += rating1 * rating2;
            }
            float similarity = numerator / (a + b - numerator); //Tanimoto coefficient
            con.write(moviePair, new Text("" + similarity));
        }
    }
}
