import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;

class IMDbRetrieval {

    private static void singleReview() throws IOException {
        Document doc = Jsoup.connect("http://www.imdb.com/title/tt0114369").get();
        Elements e = doc.getElementsByAttributeValue("itemprop", "reviewBody");
        String review = e.toString();
        review = review.substring(25, review.length()-4);
        System.out.println(review);
    }

    private static void multipleReviews()throws Exception{
        Document doc = Jsoup.connect("http://www.imdb.com/title/tt0114369/reviews").get();
        Elements e = doc.getElementsByTag("p");
        String ee = e.outerHtml();
        String[] s = ee.split("</p>\n<p>");
        for (int i=2; i<s.length-2;i++)
            System.out.println(s[i]);
    }

    public static void main(String... args)throws Exception{
        singleReview();
        multipleReviews();
    }
}