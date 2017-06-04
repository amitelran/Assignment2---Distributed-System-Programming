package recordReader;


import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.JSONObject;
 
 

/**
 * RecordReader reads <key, value> pairs from an InputSplit.
 * 
 * RecordReader, typically, converts the byte-oriented view of the input, provided by the InputSplit, 
 * and presents a record-oriented view for the Mapper and Reducer tasks for processing. 
 * It thus assumes the responsibility of processing record boundaries and presenting the tasks with keys and values.
 * 
 * Reference to help understand : https://github.com/alexholmes/hadoop-book/blob/master/src/main/java/com/manning/hip/ch3/json/JsonInputFormat.java
 */

public class TweetRecordReader extends RecordReader<TweetKey,TweetValue> { 
 
	// LineRecordReader extracts keys and values from a given text file split, 
	// where the keys are the positions of the lines and the values are the content of the line
	
    LineRecordReader reader;
        
    
    
    /***************************	 Constructor 	***************************/
    
    
    TweetRecordReader() {
        reader = new LineRecordReader(); 
    }
    
    
    
    /** Called once at initialization
     * @param split: 
     * 		InputSplit represents the data to be processed by an individual Mapper.
     * 		Typically, it presents a byte-oriented view on the input and is the responsibility of RecordReader of
     * 		the job to process this and present a record-oriented view.
     * @param context:
     * 		The context for task attempts.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        reader.initialize(split, context);
    }
 
    
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }
        
    
    
    // Return key of mapping by reading a JSON object and constructing a TweetKey object
    // The reader reads the current value/line (which is a Text object), parses it to String, and creates a new JSONObject out of this Text.
    @Override
    public TweetKey getCurrentKey() throws IOException, InterruptedException {
    	JSONObject tweetJson = new JSONObject(reader.getCurrentValue().toString());
    	System.out.println("getCurrentKey: " + reader.getCurrentValue() + "after JSON parsing: " + tweetJson);
        return new TweetKey(tweetJson);
    }
    
    
    
    
    // Return value of mapping by reading a JSON object and constructing a TweetValue object
    // The reader reads the current value/line (which is a Text object), parses it to String, and creates a new JSONObject out of this Text.
    @Override
    public TweetValue getCurrentValue() throws IOException, InterruptedException {
        try {
        	JSONObject tweetJson = new JSONObject(reader.getCurrentValue().toString());
        	System.out.println("getCurrentValue: " + reader.getCurrentValue() + "after JSON parsing: " + tweetJson);
            return new TweetValue(tweetJson);
        } 
        catch (ParseException e) {
            throw new IOException(e);
        }    
    }
    
    
    // Get the progress within the split
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }
    
    

    // Close reader
    @Override
    public void close() throws IOException {
        reader.close();        
    }
    
}
