package corpusData;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import recordReader.TweetKey;


/**
 * A vector representation for each tweet.
 * The vector should contain an entry for each word in the vocabulary (i.e., each word found in the corpus), 
 * excluding stop words, where the value of the entry is the tf-idf value of the word in the tweet.
 * (If the corpus contains 1 million words each vector will have 1 million entries)
 * we'll save/use the vectors in a SPARSE form, where we save only indices and values that are not zero.
 */

public class VectorCorpus {
	
	protected TweetKey tweet;									// The tweet for which the vector represents
	protected Path filePath;									// The path of the corpus file the vector belongs to
	protected Map<String, Integer> occurVector;					// Occurences counter vector
	protected Map<Integer, Double> sparseVector;				// Vector containing all words (by index in Corpus vector) in the tweet that occur at least once and the tf-idf value
    
    
    /*******************	Constructor		********************
     * @param path: path of the corpus file to identify with
     * @param tweet: the tweet for which the vector represents
     * @param vectorSize: the size to initialize the vector size to
     */

    
	VectorCorpus(Path path, TweetKey tweet) {
		this.tweet = tweet;
		filePath = path;
		occurVector = new HashMap<String, Integer>();			// Initialize occurrences vector
		sparseVector = new HashMap<Integer,Double>();			// Initialize tweet words sparse vector 
	}
	
	
	
	/*******************	Update/add word in Word vector	********************/
	
	
	public void updateWordVector(String term, Vector<String> stopWords, int wordIndex) {
		
		// Check that term is not a stop word
		if (!(Words.isStopWord(term, stopWords))) {
			if (!(occurVector.containsKey(term))){
				occurVector.put(term, 1);
			}
			else {
				int formerVal = occurVector.get(term);
				System.out.println("former value for term " + term + ": " + formerVal);
				occurVector.put(term, formerVal + 1);
				System.out.println("Current value for term " + term + ": " + occurVector.get(term));
			}
		}
	}
	
	
	
	/*******************	Generate Sparse Vector  	********************/
	// Key in the sparse vector : The term's index in the Corpus vector
	// Value in the sparse vector : The term's tf_idf
	
	
	public void generateSparseVector() {
		int term_index;
		double term_tf_idf;
		for (Map.Entry<String, Integer> entry : Corpus.wordsIndex.entrySet()) 
		{
			System.out.println("generateSparseVector: " + entry.getKey() + " index: " + entry.getValue());
			term_index = entry.getValue();										// Get the index of the term in the Corpus's vector
			term_tf_idf = Words.tf_idf(entry.getKey(), this.tweet);				// Get the term's tf_idf value for the tweet
			this.sparseVector.put(term_index, term_tf_idf);		    
		}
	}
	
	
	
	/*****************************	Getters	 ********************************/
	
	
	public TweetKey getTweet() { return this.tweet; }
	public Path getFilePath() { return this.filePath; }
	public Map<String, Integer> getOccurrenceVector() { return this.occurVector; }
	public Map<Integer, Double> getSparseVector() { return this.sparseVector; }



}
