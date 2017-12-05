package MovieRecommenderMapreduce;

class MovieRating {
    private String movie;
    private String rating;

    MovieRating(String movie, String rating) {
        this.movie = movie;
        this.rating = rating;
    }

    String getMovie() {
        return movie;
    }

    String getRating() {
        return rating;
    }
}
