import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJob extends Configured implements Tool {
	public static Set<String> goodWords = new HashSet<String>();
	public static Set<String> badWords = new HashSet<String>();

    public static class MapClass extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Text> {

    	private Text movie_id = new Text();
    	private Text body = new Text();

        public void map(LongWritable key, Text value,
                        OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException {
        	String[] line = value.toString().split("\t");
        	// set movie_id and body acc to the data set
	            output.collect(movie_id,body);
        	}
        }
    }

    public static class Reduce extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output,
                           Reporter reporter) throws IOException {
            //Total sentiment words count
	    int count = 0;
            String[] reviews;
            //Iterating over each review -- if more than one
            while (values.hasNext()) {
            	//remove all non whitespace and non characters from review body, split by spaces between words
	            reviews = values.next().toString().replaceAll("[^\\p{L}\\p{Z}]","").split(" ");
                for (String word : reviews) {
                	
                	if (goodWords.contains(word)) {
                		count = count+1;
                	}
                	if (badWords.contains(word)) {
                		count = count-1;
                	}
                }
            }
            if (count > 0) {
                result.set("Positive Sentiment");
            }
            else if (count < 0) {
            	result.set("Negative Sentiment");
            }
            else {
            	result.set("Neutral Sentiment");
            }
            // ******still need to decide how to output this part******
            
            output.collect(key, result);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        JobConf job = new JobConf(conf, MyJob.class);
    	positiveList(args[1]); //Path to positive words file
    	negativeList(args[0]); //Path to negative words file
        Path in = new Path(args[2]);
        Path out = new Path(args[3]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("MyJob");
        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        JobClient.runJob(job);

        return 0;
    }

    private void positiveList(String p) {
   		try {
   			BufferedReader fis = new BufferedReader(new FileReader(new File(p)));
   			String word;
   			while ((word = fis.readLine()) != null) {
   				goodWords.add(word);
          
   			}
   			fis.close();
   		} catch (IOException ioe) {
  			System.err.println("Caught exception..File not found");
   		}
   	}

   	private void negativeList(String p) {
  		try {
  			BufferedReader fis = new BufferedReader(new FileReader(new File(p)));
  			String word;
  			while ((word = fis.readLine()) != null) {
  				badWords.add(word);
  			}
  			fis.close();
  		} catch (IOException ioe) {
  			System.err.println("Caught exception..File not found");
  		}
  	}

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJob(), args);

        System.exit(res);
    }
}