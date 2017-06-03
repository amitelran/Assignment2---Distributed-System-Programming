package corpusData;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import recordReader.TweetKey;


/**
 * A vector representation for each tweet.
 * The vector should contain an entry for each word in the vocabulary (i.e., each word found in the corpus), 
 * excluding stop words, where the value of the entry is the tf-idf value of the word in the tweet.
 * (If the corpus contains 1 million words each vector will have 1 million entries)
 */

public class VectorCorpus {
	
	protected TweetKey tweet;									// The tweet for which the vector represents
	protected Path filePath;									// The path of the corpus file the vector belongs to
    protected Map<String, Integer> wordMap;						// <key = word in the vocabulary, value = tf-tdf value of the word)
    
    
    
    /*******************	Constructor		********************
     * Generates mapping for <word, tf_idf>
     * @param path: path of the corpus file to identify with
     * @param tweet: the tweet for which the vector represents
     */

    
	VectorCorpus(Path path, TweetKey tweet) {
		this.tweet = tweet;
		filePath = path;
		wordMap = new HashMap<String, Integer>();
	}
	
	
	
	// Add or update word in Word vector
	public void updateWordVector(String term, int wordCount) {
		if (!isStopWord(term)){
			int tf_idf = tf_idf_calc(term);
			if (!(wordMap.containsKey(term))){
				wordMap.put(term, tf_idf);
			}
			else {
				
			}
		}
		
	}
	
	
	
	/*****************************	Getters	 ********************************/
	
	
	public TweetKey getTweet() { return this.tweet; }
	public Path getFilePath() { return this.filePath; }
	public Map<String, Integer> getWordMap() { return this.wordMap; }



}
