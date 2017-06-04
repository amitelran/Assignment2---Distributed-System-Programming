package corpusData;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import recordReader.TweetKey;

public class Corpus {

	protected Path filePath;														// The path of the corpus file the vector belongs to
	protected static Vector<Integer> corpusVector;									// Vector for all words occurrences counter in the corpus
	protected static Map<String, Integer> wordsIndex;								// Mapping of all words and their correspnding index in the vectors
	protected static Map<TweetKey, VectorCorpus> vectorMap;							// Corpus's map of tweets and their corresponding vectors
	
	
	
	/*******************	Constructor		******************** 
	 
	 * gets file path of the corpus file
	 * creates vector for all words in corpus
	 * creates mapping of each tweet to its corresponding vector
	 * @param path
	 */
	
	Corpus(Path path) {
		corpusVector = new Vector<Integer>();
		filePath = path;
		vectorMap = new HashMap<TweetKey, VectorCorpus>();
		wordsIndex = new HashMap<String, Integer>();
	}
	
	
	
	/*******************	Add new tweet to the tweet mapping		********************
	 * Add new tweet to the tweets Map (and create tweet's new vector)
	 * @param tweetkey
	 */

	public void addTweet(TweetKey tweetkey) {
		if (!(vectorMap.containsKey(tweetkey))) {
			VectorCorpus tweetVector = new VectorCorpus(tweetkey);
			vectorMap.put(tweetkey, tweetVector);
		}
	}
	
	
	
	/*******************	Add or update word in corpus (increments by one)	********************
	 * @param term: the word to update its counter
	 * Return the word index
	 * No need to check if the term is a stop word since already checked in the VectorCorpus
	 */
	
	
	public static void updateWordCorpus(String term, Vector<String> stopWords) {
			
		// If the words map doesn't contain the word, initialize counter by 1, and add to mapping
		if (!(wordsIndex.containsKey(term))){
			corpusVector.add(1);
			wordsIndex.put(term, corpusVector.size()-1);
		}
		
		// Else, increment the word counter by 1
		else { 
			int currVal = corpusVector.elementAt(wordsIndex.get(term));			// Get the current value at the index in the vector
			corpusVector.set(wordsIndex.get(term), currVal + 1);				// Increment element at index 'wordsIndex.get' by 1
		}
	}
	
	
	
	/**************************		Getters		*****************************/
	
	
	public Path getFilePath() { return this.filePath; }
	public Vector<Integer> getVector() { return Corpus.corpusVector; }
	public Map<TweetKey, VectorCorpus> getTweetMap() { return Corpus.vectorMap; }
	public Map<String, Integer> getWordsIndexMap() { return Corpus.wordsIndex; }
	
}
