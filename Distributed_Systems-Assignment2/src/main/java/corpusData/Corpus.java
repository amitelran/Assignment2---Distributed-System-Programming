package corpusData;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import recordReader.TweetKey;

public class Corpus {

	protected Path filePath;												// The path of the corpus file the vector belongs to
	protected VectorCorpus corpusVector;									// Words vector for all words in the corpus
	protected Map<TweetKey, VectorCorpus> vectorMap;						// Corpus's map of tweets and their corresponding vectors
	
	
	
	/*******************	Constructor		******************** 
	 
	 * gets file path of the corpus file
	 * creates vector for all words in corpus
	 * creates mapping of each tweet to its corresponding vector
	 * @param path
	 */
	
	Corpus(Path path) {
		corpusVector = new VectorCorpus(path, null);
		filePath = path;
		vectorMap = new HashMap<TweetKey, VectorCorpus>();
	}
	
	
	
	/*******************	Add new tweet to the tweet mapping		********************
	 * Add new tweet to the tweets Map (and create tweet's new vector)
	 * @param tweetkey
	 */

	public void addTweet(TweetKey tweetkey) {
		if (!(vectorMap.containsKey(tweetkey))) {
			VectorCorpus tweetVector = new VectorCorpus(this.filePath, tweetkey);
			vectorMap.put(tweetkey, tweetVector);
		}
	}
	
	
	
	/*******************	Add or update word in corpus (increments by one)	********************
	 * @param term: the word to update its counter
	 */
	
	
	public void updateWordCorpus(String term) {
		if (!isStopWord(term)){
			
			// If the vector doesn't contain the word, initialize counter by 1
			if (!(corpusVector.wordMap.containsKey(term))){
				corpusVector.updateWordVector(term, 1);
			}
			
			// Else, increment the word counter by 1
			else {
				corpusVector.wordMap.put(term, corpusVector.wordMap.get(term) + 1);
			}
		}
	}
	
	
	
	/**************************		Getters		*****************************/
	
	
	public Path getFilePath() { return this.filePath; }
	public VectorCorpus getVector () { return this.corpusVector; }
	public Map<TweetKey, VectorCorpus> getTweetMap() { return this.vectorMap; }
	
}
