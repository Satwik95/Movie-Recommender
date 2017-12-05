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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

class RecommendationFinder {
    static HashMap<String,Integer> movieCount = new HashMap<>();
    static HashMap<String,String[]> movies = new HashMap<>();
    static HashMap<String,String> movieNames = new HashMap<>();
    static int totalMovies=0,totalUsers=0,noRecommendations=0;

    static void main(String... args) throws Exception {
        Configuration c = new Configuration();
        new GenericOptionsParser(c, args).getRemainingArgs();
        Path input1 = new Path("output1/part-r-00000");
        Path input2 = new Path("output4/part-r-00000");
        Path input3 = new Path("movies.csv");
        Path output = new Path("output");
        Job j = new Job(c, "RecommendationFinder");
        FileSystem fs = FileSystem.get(c);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        j.setJarByClass(MovieRecommender.class);
        MultipleInputs.addInputPath(j, input1, TextInputFormat.class, MapForLoadingUsers.class);
        MultipleInputs.addInputPath(j, input2, TextInputFormat.class, MapForLoadingMovies.class);
        MultipleInputs.addInputPath(j, input3, TextInputFormat.class, MapForLoadingMovieNames.class);
        j.setReducerClass(ReduceForRecommendationFinder.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(j, output);
        j.waitForCompletion(true);
    }

    private static class MapForLoadingUsers extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable key, Text u, Context con) throws
                IOException, InterruptedException {
            String user = u.toString().substring(0,u.toString().indexOf('\t'));
            String movies = u.toString().substring(u.toString().indexOf('\t')+1);
            con.write(new Text(user), new Text(movies));
            RecommendationFinder.totalUsers++;
        }
    }

    private static class MapForLoadingMovies extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable key, Text m, Context con) throws
                IOException, InterruptedException {
            String movie = m.toString();
            String[] movies = movie.substring(movie.indexOf('\t')+1).split(",");
            RecommendationFinder.movies.put(movie.substring(0,movie.indexOf('\t')),movies);
            RecommendationFinder.totalMovies++;
        }
    }

    private static class MapForLoadingMovieNames extends Mapper<LongWritable, Text,
            Text, Text> {
        public void map(LongWritable key, Text m, Context con) throws
                IOException, InterruptedException {
            String movie = m.toString();
            String movieId = movie.substring(0,movie.indexOf(','));
            movie=movie.substring(movie.indexOf(',')+1);
            if (movie.charAt(0)=='"')
                movie = movie.substring(1,movie.indexOf('"',1));
            else
                movie = movie.substring(0,movie.indexOf(','));
            RecommendationFinder.movieNames.put(movieId,movie);
            MovieRecommender.movies.add(movieId);
        }
    }

    private static class ReduceForRecommendationFinder extends Reducer<Text,
            Text, Text, Text> {

        String[] movieList = new String[0];
        ArrayList<String> recommendedMovieList = new ArrayList<>();
        ArrayList<String> referringMovieList = new ArrayList<>();


        public void reduce(Text user, Iterable<Text> user_movies, Context
                con) throws IOException, InterruptedException {
            int n = MovieRecommender.n;
            referringMovieList.clear();
            for (Text s : user_movies) {
                movieList = s.toString().split(",");
            }
            for (int i = 0; i < movieList.length; i++) {
                String movie = movieList[i].substring(0,movieList[i].indexOf(' '));
                float rating = Float.parseFloat(movieList[i].substring(1+movieList[i].indexOf(' ')));
                movieList[i]=movie;
                if (rating>=4.5)
                    referringMovieList.add(movie);
            }
            recommendedMovieList.clear();
            for (String movie:referringMovieList)
                Collections.addAll(recommendedMovieList, movies.get(movie));
            recommendedMovieList.removeAll(Arrays.asList(movieList));
            movieList = recommendedMovieList.toArray(new String[recommendedMovieList.size()]);
            Arrays.sort(movieList);
            recommendedMovieList.clear();
            int k=0;
            int[][] frequency = new int[movieList.length][2];
            if (movieList.length==0) {
                con.write(user, new Text("No recommendations"));
                noRecommendations++;
                return;
            }
            frequency[0][0]=1;
            frequency[0][1]=0;
            for (int i = 1; i < movieList.length; i++)
                if (movieList[i].equals(movieList[i - 1]))
                    frequency[k][0]++;
                else {
                    frequency[++k][1] = i;
                    frequency[k][0] = 1;
                }
            Arrays.sort(frequency, new FrequencyComparator());
            int max_count=frequency[0][0];
            int a=0,b=0;
            while (n>0 && max_count>0) {
                int count = 0;
                while (a < frequency.length && frequency[a++][0] == max_count)
                    count++;
                if (count <= n) {
                    for (int i = b; i < a; i++) {
                        recommendedMovieList.add(movieList[frequency[i][1]]);
                        n--;
                    }
                } else {
                    int[] num = randomize(count);
                    for (int i = 0; i < n; i++)
                        recommendedMovieList.add(movieList[frequency[b + num[i]][1]]);
                    break;
                }
                b = a;
                max_count = frequency[a][0];
            }
            con.write(user,getMovieNames());
        }

        private int[] randomize(int x){
            int[] num = new int[x];
            for (int i=0;i<x;i++)
                num[i]=i;
            Collections.shuffle(Arrays.asList(num));
            return num;
        }

        private Text getMovieNames(){
            StringBuilder list= new StringBuilder("\n");
            for (String movie:recommendedMovieList) {
                list.append("\t\t").append(movieNames.get(movie)).append("\n");
                MovieRecommender.movies.remove(movie);
                RecommendationFinder.movieCount.merge(movieNames.get(movie), 1, Integer::sum);
            }
            return new Text(list.toString());
        }
    }
}
