package createVectors;

import java.io.DataInput;
import java.io.DataOutput;
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


	/**********************		writableArray_A	**********************/


	public static class writableArray_A implements Writable {

		private TweetKey tweetKey;
		private TweetValue tweetValue;
		private int wordOccurInTweet;
		private int maxOccur;

		// Default constructor to allow (de)serialization
		writableArray_A() {}
		
		writableArray_A(TweetKey tweetKey, TweetValue tweetValue, int wordOccurTweet, int maxOccurrences) {
			this.tweetKey = tweetKey;
			this.tweetValue = tweetValue;
			this.wordOccurInTweet = wordOccurTweet;
			this.maxOccur = maxOccurrences;
		}

		writableArray_A(DataInput in) throws IOException {
			this.readFields(in);
		}

		public void write(DataOutput out) throws IOException {
			tweetKey.write(out);
			tweetValue.write(out);
			out.writeInt(wordOccurInTweet);
			out.writeInt(maxOccur);
		}


		public void readFields(DataInput in) throws IOException {
			tweetKey.readFields(in);
			tweetValue.readFields(in);
			wordOccurInTweet = in.readInt();
			maxOccur = in.readInt();
		}
		
		
		public TweetKey getTweetKey() { return this.tweetKey; }
		public TweetValue getTweetValue() { return this.tweetValue; }
		public int getWordOccurInTweet() { return this.wordOccurInTweet; }
		public int getMaxOccur() { return this.maxOccur; }
	}


	
	/**********************		Mapper_A	**********************/
	// Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	// From RecordReader: KEYIN = TweetKey, VALUEIN = TweetValue 
	// To Reducer: KEYOUT = word (Text), VALUEOUT = tuple (TweetKey, TweetValue, number of times word appeared in tweet, maximal number of times any word appeared in the tweet)


	public static class Mapper_A extends Mapper<TweetKey, TweetValue, Text, writableArray_A> {

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
				writableArray_A writArr = new writableArray_A(tweetKey, tweetValue, entry.getValue(), maxOccur);
				//Writable[] writArr = { tweetKey, tweetValue, new IntWritable(3), new IntWritable(maxOccur) };
				//TupleWritable tweetTuple = new TupleWritable(writArr);
				context.write(entry.getKey(), writArr);
			}
		}
	}



	/**********************		REDUCER_A	**********************/
	// Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	// From Mapper_A: KEYIN = TweetKey, VALUEIN = CorpusVector that corresponds to the tweetKey
	// To output: KEYOUT = word, VALUEOUT = accumulated number of occurrences in tweet


	public static class Reducer_A extends Reducer<Text, writableArray_A, LongWritable, TupleWritable> {

		long numOfTweets;

		@Override
		protected void setup(Reducer<Text, writableArray_A, LongWritable, TupleWritable>.Context context) throws IOException, InterruptedException {
			Counter counter = context.getCounter(TaskCounter.MAP_INPUT_RECORDS);
			numOfTweets = counter.getValue();
			super.setup(context);
		}

		@Override
		public void reduce(Text word, Iterable<writableArray_A> ourArrays, Context context) throws IOException,  InterruptedException {
			int tf_d;
			int max_occur;
			int num_of_tweets_contains_word = Iterables.size(ourArrays);

			double tf;
			double idf;

			double tf_idf;

			for (writableArray_A arr : ourArrays) {
				tf_d = arr.getWordOccurInTweet();
				//tf_d = ((IntWritable) tuple.get(2)).get();
				max_occur = arr.getMaxOccur();
				//max_occur = ((IntWritable) tuple.get(3)).get();

				tf = 0.5 + 0.5*(tf_d/max_occur);
				idf = Math.log(numOfTweets/num_of_tweets_contains_word);

				tf_idf = tf*idf;
				
				Writable[] wordTuple = {word,new DoubleWritable(tf_idf)};
				//context.write( new LongWritable(((TweetKey)tuple.get(0)).getID()), new TupleWritable(wordTuple)); 
				
				context.write(new LongWritable(arr.getTweetKey().getID()), new TupleWritable(wordTuple));

			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("mapred.map.tasks","10");
		//conf.set("mapred.reduce.tasks","2");

		//			File file = new File(args[2]);
		String stopWords[] = {"a", "about", "above", "across", "after", "afterwards"};
		conf.setStrings("stopWords", stopWords);


		Job job = Job.getInstance(conf, "TweetVectors");

		job.setJarByClass(CreateVectors.class);
		job.setMapperClass(Mapper_A.class);
		job.setCombinerClass(Reducer_A.class);
		job.setReducerClass(Reducer_A.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(writableArray_A.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(TupleWritable.class);
		job.setInputFormatClass(TweetInputFormat.class);
		TweetInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
