package recordReader;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
 
import org.apache.hadoop.io.WritableComparable;
import org.json.JSONObject;
 

/** A Writable which is also Comparable.
 * 
 * WritableComparables can be compared to each other, typically via Comparators. 
 * Any type which is to be used as a key in the Hadoop Map-Reduce framework should implement this interface
 */
public class TweetKey implements WritableComparable<TweetKey> {
 
    protected long id;
    protected String created_at;
            
    
    /***************	 Constructors	 ***************/
    
    
    public TweetKey(long id, String createdAt) {
        this.id = id;
        this.created_at = createdAt;
    }
    
    
    public TweetKey(JSONObject tweetJson) {
        id = tweetJson.getInt("id");
        created_at = tweetJson.getString("created_at");
    }
 
    
    /***************	 getters	 ***************/
    
    
    public long getID() { return id; }
    
    public String getCreatedAt() { return created_at; }
    
    
    /***************	 Read & Write to data methods	 ***************/
    
    
    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        created_at = in.readUTF();
    }
    
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeUTF(created_at);
    }
    
    
    
    /*******************************	 Comparing method	 *******************************/

    
    // Compare between keys of TweetKey objects
    @Override
    public int compareTo(TweetKey otherTweetKey) {
    	
    	// If the 'created_at' fields are equal, check for the 'id' fields
        if (this.created_at.equals(otherTweetKey.created_at)) {
        	return (int)(this.id - otherTweetKey.id);
        }
        
        // If the 'created_at' fields are not equals, return something to indicate inequality
        else {
        	return this.created_at.compareTo(otherTweetKey.created_at);
        }
    }
    
}