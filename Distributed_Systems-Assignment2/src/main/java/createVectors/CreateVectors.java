package createVectors;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Iterables;

import corpusData.Words;
import recordReader.TweetInputFormat;
import recordReader.TweetKey;
import recordReader.TweetValue;
import org.apache.hadoop.io.LongWritable;



// Helpful reference:
// https://www.dezyre.com/hadoop-tutorial/hadoop-mapreduce-wordcount-tutorial

public class CreateVectors { 


	/**********************		Mapper_A	**********************/
	// Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	// From RecordReader: KEYIN = TweetKey, VALUEIN = TweetValue 
	// To Reducer: KEYOUT = word (Text), VALUEOUT = tuple (TweetKey, TweetValue, number of times word appeared in tweet, maximal number of times any word appeared in the tweet)


	public static class Mapper_A extends Mapper<TweetKey, TweetValue, Text, TupleWritable> {

		private Text word = new Text();										// Each word in the text

		// The map function gets tweetKey & tweetValue. 
		// It generates CorpusVector object that corresponds to the given tweetKey.
		// In particular, it updates the occurrencesVector mapping inside the CorpusVector.

		@Override
		public void map(TweetKey tweetKey, TweetValue tweetValue, Context context) throws IOException,  InterruptedException {

			// Set stop words list from Strings array in configuration
			List<String> stopWords = Arrays.asList(context.getConfiguration().getStrings("stopWords"));

			int maxOccur = 0;
			Integer currVal = 0;
			Map<Text, Integer> occurMap = new HashMap<Text, Integer>();

			String curWord;
			String tweetText = tweetValue.getText();
			StringTokenizer itr = new StringTokenizer(tweetText);

			// Iterate through all words in tweet text
			while (itr.hasMoreTokens()) {
				curWord = itr.nextToken(); 
				word.set(curWord);		

				// Check that word is not a stop word (if it is, ignore it)
				if (!(stopWords.contains(word.toString()))) {	
					currVal = occurMap.get(curWord);
					if(currVal == null)
						currVal = 0;
					occurMap.put(word, currVal+1);
				}
				// If needed, update maximal number of occurrences 
				if (maxOccur < currVal.intValue()) {
					maxOccur = currVal;
				}
			}

			for (Map.Entry<Text, Integer> entry : occurMap.entrySet())
			{
				Writable[] writArr = { tweetKey, tweetValue, new IntWritable(occurMap.get(word.toString())), new IntWritable(maxOccur) };
				TupleWritable tweetTuple = new TupleWritable(writArr);
				context.write(entry.getKey(), tweetTuple);
			}
		}
	}



		/**********************		REDUCER_A	**********************/
		// Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
		// From Mapper_A: KEYIN = TweetKey, VALUEIN = CorpusVector that corresponds to the tweetKey
		// To output: KEYOUT = word, VALUEOUT = accumulated number of occurrences in tweet


		public static class Reducer_A extends Reducer<Text, TupleWritable, LongWritable, TupleWritable> {
			
			long numOfTweets;
			
			@Override
			protected void setup(Reducer<Text, TupleWritable, LongWritable, TupleWritable>.Context context)
					throws IOException, InterruptedException {
				Counter counter = context.getCounter(TaskCounter.MAP_INPUT_RECORDS);
				numOfTweets = counter.getValue();
				super.setup(context);
			}
			
			@Override
			public void reduce(Text word, Iterable<TupleWritable> tuples, Context context) throws IOException,  InterruptedException {
				int tf_d;
				int max_occur;
				int num_of_tweets_contains_word = Iterables.size(tuples);
				
				double tf;
				double idf;
				
				double tf_idf;
				
				for (TupleWritable tuple : tuples) {
					tf_d = ((IntWritable) tuple.get(2)).get();
					max_occur = ((IntWritable) tuple.get(3)).get();
					
					tf = 0.5 + 0.5*(tf_d/max_occur);
					idf = Math.log(numOfTweets/num_of_tweets_contains_word);

					tf_idf = tf*idf;
					Writable[] wordTuple = {word,new DoubleWritable(tf_idf)};
					context.write( new LongWritable(((TweetKey)tuple.get(0)).getID()), new TupleWritable(wordTuple)); 
				}
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			//conf.set("mapred.map.tasks","10");
			//conf.set("mapred.reduce.tasks","2");

			File file = new File("stop_words.txt");
			String stopWords[] = Words.readStopWords(file);
			conf.setStrings("stopWords", stopWords);
			

		    Job job = Job.getInstance(conf, "TweetVectors");
		    
			job.setJarByClass(CreateVectors.class);
			job.setMapperClass(Mapper_A.class);
			job.setCombinerClass(Reducer_A.class);
			job.setReducerClass(Reducer_A.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(TupleWritable.class);
			TweetInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

	}
