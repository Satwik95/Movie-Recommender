package MovieRecommenderMapreduce;

import java.util.*;
import java.util.stream.Collectors;

public class MovieRecommender {
    static HashMap<String, Double> lexicon = new HashMap<>();
    static ArrayList<String> movies = new ArrayList<>();
    static int k=0,n=0;
    public static void main(String... args) throws Exception {
        if (args.length!=0){
            k = Integer.parseInt(args[0]);
            n = Integer.parseInt(args[1]);
        }
        if (k==0)k=Integer.MAX_VALUE;
        if (n==0)n=Integer.MAX_VALUE;

        UserRatingAgglomerator.main(args);
        SimilarityComputer.main(args);
        SentimentAnalyzer.main(args);
        ListCreator.main(args);
        RecommendationFinder.main(args);

        System.err.println("\nThe total movie coverage is: "+(1-(0.0+movies.size())/ RecommendationFinder.totalMovies)*100+"%");
        System.err.println("The total user coverage is: "+(1-(0.0+ RecommendationFinder.noRecommendations)/ RecommendationFinder.totalUsers)*100+"%");
        List<Map.Entry<String, Integer>> orderedList = RecommendationFinder.movieCount.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(10)
                .collect(Collectors.toList());
        ArrayList<String> topMovies = new ArrayList<>();
        orderedList.forEach(mov -> topMovies.add(mov.getKey()));
        System.err.println("\nTop 10 recommended movies:");
        for (String movie:topMovies)
            System.err.println(movie);
    }
}
