package recordReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.Writable;
import org.json.JSONObject;
 
 

public class TweetValue implements Writable {
        
    protected String username;
    protected String text;
    protected boolean favorited;
    protected boolean retweeted;
    
    
    /***************	 Constructor - receives JSON object and parses to a TweetValue object	 ***************/

    
    public TweetValue(JSONObject tweetJson) throws ParseException {
        username = tweetJson.getJSONObject("user").getString("name"); 
        text = tweetJson.getString("text");
        favorited = tweetJson.getJSONObject("retweeted_status").getBoolean("favorited");
        retweeted = tweetJson.getJSONObject("retweeted_status").getBoolean("retweeted");
        System.out.println("TweetValue constructor:  username = " + username + "  text = " + text);

    }
    
    
    
    /***************	 Read & Write fields of TweetValue object	 ***************/
    
    
    @Override
    public void readFields(DataInput in) throws IOException {
        username = in.readUTF();
        text = in.readUTF();
        favorited = in.readBoolean();
        retweeted = in.readBoolean();
    }
    
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(username);
        out.writeUTF(text);
        out.writeBoolean(favorited);
        out.writeBoolean(retweeted);
    }
    
    
    
    /**********************************	 Getters  **********************************/

    
    public String getUsername() { return username; }
    public String getText() { return text; }
    public boolean getFavorited() { return favorited; }
    public boolean getReTweeted() { return retweeted; }

}