import java.io.*;
import java.util.Scanner;

public class Lexicon {
    public static void main(String...args) throws IOException {
        BufferedReader sc1 = new BufferedReader(new FileReader("imdb.vocab"));
        BufferedReader sc2 = new BufferedReader(new FileReader("imdbEr.txt"));
        PrintWriter pw = new PrintWriter(new FileWriter("lexicon.csv"));
        while (true){
            String s1= sc1.readLine();
            String s2= sc2.readLine();
            if (s1==null||s2==null)
                break;
            pw.println(s1+"\t"+s2);
        }
        pw.close();
    }
}
