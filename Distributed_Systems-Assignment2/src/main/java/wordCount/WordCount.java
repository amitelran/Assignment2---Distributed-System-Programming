package wordCount;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import corpusData.VectorCorpus;
import corpusData.Words;
import recordReader.TweetKey;
import recordReader.TweetValue;

import org.apache.hadoop.io.LongWritable;



// Helpful reference:
// https://www.dezyre.com/hadoop-tutorial/hadoop-mapreduce-wordcount-tutorial

public class WordCount { 


	/**********************		Mapper_A	**********************/
	// Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	// From RecordReader: KEYIN = TweetKey, VALUEIN = TweetValue 
	// To Reducer: KEYOUT = word (Text), VALUEOUT = tuple (TweetKey, TweetValue, number of times word appeared in tweet, maximal number of times any word appeared in the tweet)


	public static class Mapper_A extends Mapper<TweetKey, TweetValue, TweetKey, TupleWritable> {

		private Text word = new Text();										// Each word in the text

		// The map function gets tweetKey & tweetValue. 
		// It generates CorpusVector object that corresponds to the given tweetKey.
		// In particular, it updates the occurrencesVector mapping inside the CorpusVector.
		
		@Override
		public void map(TweetKey tweetKey, TweetValue tweetValue, Context context) throws IOException,  InterruptedException {
			
			// Set stop words list from Strings array in configuration
			List<String> stopWords = Arrays.asList(context.getConfiguration().getStrings("stopWords"));
			
			int maxOccur = 0;
			Integer currVal;
			Map<String, Integer> occurMap = new HashMap<String, Integer>();
			
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
					occurMap.put(word.toString(), currVal++);
				}
				// If needed, update maximal number of occurrences 
				if (maxOccur < currVal) {
					maxOccur = currVal;
				}
			}			
			
			for (Map.Entry<String, Integer> entry : occurMap.entrySet())
			{
				Writable[] writArr = { tweetKey, tweetValue, new IntWritable(occurMap.get(word.toString())), new IntWritable(maxOccur) };
				TupleWritable tweetTuple = new TupleWritable(writArr);
				context.write(tweetKey, tweetTuple);
			}
	}


	
	/**********************		REDUCER_A	**********************/
	// Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	// From Mapper_A: KEYIN = TweetKey, VALUEIN = CorpusVector that corresponds to the tweetKey
	// To output: KEYOUT = word, VALUEOUT = accumulated number of occurrences in tweet


	public static class Reducer_A extends Reducer<TweetKey, VectorCorpus, Text, IntWritable> {
		
		
		@Override
		public void reduce(TweetKey tweetKey, VectorCorpus tweetVector, Context context) throws IOException,  InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum)); 
		}
	}

	
	/**********************		PARTITIONER_A   	**********************/
	// Partitioner <KEY, VALUE>
	// From Mapper_A: KEY = TweetKey, VALUE = CorpusVector


	public static class Partitioner_A extends Partitioner<TweetKey, VectorCorpus> {
		
		// Get the partition number for a given key (hence record) given the total number of partitions ( = total number of reducers)
		@Override
		public int getPartition(TweetKey tweetKey, VectorCorpus tweetVector, int numPartitions) {
			return getLanguage(key) % numPartitions;
		}

		
		private int getLanguage(Text key) {
			if (key.getLength() > 0) {
				int c = key.charAt(0);
				if (c >= Long.decode("0x05D0").longValue() && c <= Long.decode("0x05EA").longValue())
					return 1;
			}
			return 0;
		}
	}



	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","10");
		//conf.set("mapred.reduce.tasks","2");

		File file = new File("C:\\Users\\Amir\\Desktop\\stop_words.txt");
		ArrayWritable stopWordsArray = new ArrayWritable(Words.readStopWords(file));
		//conf.set("stopWords", stopWordsArray);			// Set path to file in configuration
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(Mapper_A.class);
		job.setPartitionerClass(Partitioner_A.class);
		job.setCombinerClass(Reducer_A.class);
		job.setReducerClass(Reducer_A.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
