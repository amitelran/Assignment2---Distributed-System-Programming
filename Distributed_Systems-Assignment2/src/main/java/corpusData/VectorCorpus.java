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
	
	
	
	/*******************	Calculate cosine similarity between two vectors  	********************/
	// Key in the sparse vector : The term's index in the Corpus vector
	// Value in the sparse vector : The term's tf_idf
	
	
	public double cosine_Similarity(Map<Integer, Double> sparseVector_A, Map<Integer, Double> sparseVector_B) {
		double num = 0;
		double vector_A_squared_sum = 0;
		double vector_B_squared_sum = 0;
		double vector_A_squared_sum_root = 0;
		double vector_B_squared_sum_root = 0;
		double sum_vectors_components = 0;
		
		
		/* ************* Calculate vector_A sum of squares squared root ************* */
		
		
		for (Map.Entry<Integer, Double> entry : sparseVector_A.entrySet()) 
		{
			System.out.println("cosine_Similarity vector A: " + entry.getKey() + " tf_idf: " + entry.getValue());
			
			num = entry.getValue();												// Get the tf_idf
			num = (num * num);													// Square it
			vector_A_squared_sum += num;										// Sum of squares
		}
		vector_A_squared_sum_root = Math.sqrt(vector_A_squared_sum);
		
		
		/* ************* Calculate vector_B sum of squares squared root ************* */
		
		
		for (Map.Entry<Integer, Double> entry : sparseVector_B.entrySet()) 
		{
			System.out.println("cosine_Similarity vector B: " + entry.getKey() + " tf_idf: " + entry.getValue());
			
			num = entry.getValue();												// Get the tf_idf
			num = (num * num);													// Square it
			vector_B_squared_sum += num;										// Sum of squares 
		}
		vector_B_squared_sum_root = Math.sqrt(vector_B_squared_sum);
		
		
		/* ************* If any of the rooted sums is 0 somehow, return 0 ************* */
		
		
		if ((vector_A_squared_sum_root == 0) || (vector_B_squared_sum_root == 0)) {
			return 0;
		}
		
		
		/* ************* Iterate through vector_A, and for each component, look for corresponding component in B (if exists) and sum ************* */
		
		
		for (Map.Entry<Integer, Double> entry : sparseVector_A.entrySet()) 
		{
			if (!(sparseVector_B.containsKey(entry.getKey()))) {									// Check that the key in vector A (representing a term) exists in vector B
				continue;
			}
			sum_vectors_components += (entry.getValue() + sparseVector_B.get(entry.getKey()));   	// Sum of corresponding components in the sparse vectors
		}
		
		
		/* ************* Return cosine similarity ************* */
		
		
		return (sum_vectors_components / (vector_A_squared_sum_root * vector_B_squared_sum_root));
	}
	
	
	
	/*****************************	Getters	 ********************************/
	
	
	public TweetKey getTweet() { return this.tweet; }
	public Path getFilePath() { return this.filePath; }
	public Map<String, Integer> getOccurrenceVector() { return this.occurVector; }
	public Map<Integer, Double> getSparseVector() { return this.sparseVector; }



}
