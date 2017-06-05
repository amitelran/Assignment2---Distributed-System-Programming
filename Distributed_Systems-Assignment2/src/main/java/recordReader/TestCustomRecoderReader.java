package recordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;



public class TestCustomRecoderReader {

	public static void main(String[] args) throws IOException, InterruptedException 
	{
		Configuration conf = new Configuration(false);
		//conf.set("fs.default.name", "file:///");

		File testFile = new File("C:\\Users\\Amir\\Desktop\\testCorpus.json");
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		System.out.println("Check1");

		TweetInputFormat tweetInputFormat = ReflectionUtils.newInstance(TweetInputFormat.class, conf);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		RecordReader reader = tweetInputFormat.createRecordReader(split, context);

		System.out.println("Check2");

		reader.initialize(split, context);
		reader.getCurrentKey();
		reader.getCurrentValue();
	}

}


