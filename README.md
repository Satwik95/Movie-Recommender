# Movie-Recommender
Movie Recommendation system based on collaborative filtering and sentiment analysis.
 The explosive increasing of the data on the Web has created and promoted the development of data mining are and is welcomed by researchers from both academia and industry. The sentiment computing of text like movie reviews is a signicant component of the social media. It has also attracted a lot of researches, which could support many real-world applications, such as public opinion monitoring and recommendation for Websites. This paper proposes a recommendation system with a combination of collaborative ﬁltering and sentiment analysis. The challenges of big data are solved using Hadoop through map reduce framework where the complete data is mapped and reduced to smaller sizable data to ease of handling.

# Methodology :
We use the MovieLens dataset from GroupLens consisting of 100,004 ratings of 9,125 movies by 671 users. A lexicon customized for movie reviews is used for sentiment analysis with each word in the lexicon assigned a value with the higher (positive) values indicating a more positive sentiment and the lower (negative) values indicating a negative sentiment. Up to 10 reviews for each movie was scraped from IMdB (Internet Movie Database). Due to the sheer size of the operations, we execute it on Hadoop using map-reduce. 

# Integrating ratings by each user:
In the dataset ratings.csv in MovieLens, we have the user-ID, the movie-ID, the rating by that user to that movie (on a scale of 1-5) and the timestamp as shown in 1. We integrate all the individual ratings by a user into a long string containing the movie-ID and the rating. The user-to-movie rating relationship is mapped in the mapper and the movie and ratings are concatenated in the reducer.

# Computing similarity between movie pairs
In this phase, we ﬁnd the similarity between each pair of movies. The input is the output of the previous map-reduce job. Mapper of the map-reduce job is responsible for building relationships among movies i.e. it is responsible for emitting the key-value pairs where each key is a set of two movies and the value represents the rating given by a user for both the movies. Reducer of the map-reduce job is responsible for computing the similarity among the movies being emitted by the mapper i.e. it is responsible for computing the similarity of each key represented by pair of movies. We use Tanimoto co-eﬃcient for computing similarity which is given by:
f(A,B) = A·B /(|A|^2 +|B|^2 + A·B), where A and B are vectors.

# Finding the sentiment of the reviews of each movie
We have collected upto 10 reviews for each movie using web scraping which are in the format ”movie-ID, review”. The mapper computes the sentiment of each review based on the lexicon by the average sentiment of a word in the review given by:
S(R) = SUM(S(wi))/|w| , where, S(R) = sentiment of the review, wi = word i in the review, |w| = no. of words in the review, and s(wi) = sentiment of wi The sentiment score is mapped as the value to the movie-ID as key. Reducer of the map-reduce job simply averages the sentiment score across the reviews for each movie.

# Constructing Movie Lists
This map-reduce job is responsible for deriving the recommendations based on the similarity values between the movie pairs computed in Section 5.2. The map job reads each key which is an item pair and the value which is the similarity value among the item pairs. It then emits the key into two individual items i.e. if the key is say (I1,I2), it emits the pair into I1 and I2. After emitting the key pairs, it merges the items in a way such that each item behaves as an independent key and the value for each item is an another item of the item pair along with the similarity value between the item pair i.e. say (I1,I2) is an item pair and the similarity value among the item pair is 4.5, the mapper output for the item pair (I1,I2) with similarity value 4.5 is (I1, I2-4.5) and (I2, I1-4.5) as shown in 5. The reduce job takes in collection of values of the item key. We get a list of movies and their respective similarities with the movie under consideration. We calculate the score of each of the movies in the list as a function of the similarity and the sentiment value. i.e Score = f(Similarity,Sentiment). The list of movies is then sorted in descending order of scores and the top ‘k’ selected and stored for the movie under consideration. This list is the top ‘k’ movies to be recommended to users who have watched that movie and this process takes place for each of the movies in the datase.

#  Deriving recommendations
In the ﬁnal map-reduce job, we recommend a list of ‘n’ movies for each user. The inputs to this are the outputs of the ﬁrst and the fourth map-reduce jobs (in Section 5.1 and 5.3). The respective mappers simply construct hashmaps from the inputs. In the reducer, we ﬁrst make a list of the movies the user has rated (say Movies List). Next we ﬁlter out the movies which the user has rated 4.5 or above i.e. the movies the user really liked (say Referral List). We remove the ratings from Movies List and retain just the movie-IDs. Next, we make a list of all the recommended movies for the movies in Referral List (say Recommended List). Then we remove all the movies from Recommended List which are in Movies List since those movies have already been rated by the user. Next, we ﬁnd the frequency of each movie in Recommended List. We form our ﬁnal list of ‘n’ recommended movies in order of their frequency.

# Authors:
* **Satwik Mishra**
* **Rishabh Agarwal**
