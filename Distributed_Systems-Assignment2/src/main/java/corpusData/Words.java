package corpusData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import recordReader.TweetKey;


public class Words {
	
	
	/***************	Build stop words mapping	 ***************/
	
	// Receive file containing stop words and map to insert the mapping of stop words into as parameters
	// Map all stop words in file to the input map
	
	
	public static String[] readStopWords(File file) throws FileNotFoundException, IOException {
		Scanner sc = new Scanner(file);
		List<String> lines = new ArrayList<String>();
		while (sc.hasNextLine()) {
			lines.add(sc.nextLine());
		}
		String[] stopWordsArray = lines.toArray(new String[0]);
		return stopWordsArray;
	}
	
	
	
	/***************	Calculate tf	 ***************/
	
	// tf - term frequency (number of times the term occurs in the corpus)
	// augmented frequency, to prevent a bias towards longer documents,
	// e.g. raw frequency divided by the maximum raw frequency of any term in the document
	
	
	public static int tf (String term, TweetKey tweet) {
		int tf = numTermOccur_InCorpus(term);											// Find number of times the term occurs in the whole corpus
		int maxOccur = findMax(Corpus.vectorMap.get(tweet).getOccurrenceVector());		// Find maximal number of occurrences for a single word in the tweet
		tf /= maxOccur;																	// Divide the term occurrences my the max occurrences
		tf *= (1/2);																	// Multiply by 1/2
		tf += (1/2);																	// Add 1/2
		return tf;
	}
	
	
	
	/***************	Calculate df	 ***************/

	// idf - inverse document frequency (whether the term is common or rare across all documents)
	// It is the logarithmically scaled inverse fraction of the documents that contain the word, 
	// obtained by dividing the total number of documents by the number of documents containing the term, 
	// and then taking the logarithm of that quotient
	
	
	public static double idf (String term) {
		double idf = 0;
		int numOfTweets = Corpus.vectorMap.size();								// Get total number of documents
		int numOfTweets_ContainingTerm = 0;
		
		// Iterate through all tweets vectors and count the number of documents containing the term
		for (Map.Entry<TweetKey, VectorCorpus> entry : Corpus.vectorMap.entrySet()) 
		{
			System.out.println("idf: " + entry.getKey().getCreatedAt());
			if (entry.getValue().occurVector.containsKey(term)) {				// Check if the term occurs in the occurence vector
				numOfTweets_ContainingTerm++;
			}	    
		}
		
		// Avoid dividing by zero in case no tweet contains the term
		if (numOfTweets_ContainingTerm == 0) {
			numOfTweets_ContainingTerm = 1;
		}
		idf = Math.log(numOfTweets / numOfTweets_ContainingTerm);				// Perform log operation over the variants
		return idf;
	}
	
	
	
	/***************	Calculate tf_idf	 ***************/
	
	// tf-idf is basically the frequency of a term (word) multiplied by its inverse document frequency (to reduce the value 
	// of non-meaningful words that appear a lot). It reflects the importance of a term in a tweet. 
	// Since tweets contain no more than 140 characters, most of the vector will contain zeros; 
	// therefore, we'll save/use the vectors in a non-sparse form, where we save only indices and values that are not zero.
	
	
	public static double tf_idf (String term, TweetKey tweet) {
		int tf = tf(term, tweet);
		double idf = idf(term);
		return (tf * idf);
	}
	
	
	
	/***************	Check if the stopWords vector contains the term	 ***************/
	
	
	public static boolean isStopWord(String term, Vector<String> stopWords) {
		return stopWords.contains(term);
	}
	
	
	
	/***************	Find maximal number of occurrences for a single word in a single tweet	 ***************/
	
	
	public static int findMax(Map<String, Integer> tweetVector) {
		int maxVal = 0;
		for (Map.Entry<String, Integer> entry : tweetVector.entrySet()) 
		{
			System.out.println(entry.getKey() + "/" + entry.getValue());
			if (maxVal < entry.getValue()) {
				maxVal = entry.getValue();
			}		    
		}
		return maxVal;
	}
	
	
	
	/***************	 Get number of times the specified term occurs in the corpus	 ***************/
	
	
	public static int numTermOccur_InCorpus(String term) {
		int termIndex = Corpus.wordsIndex.get(term);
		System.out.println("numTermOccur_InCorpus: " + Corpus.corpusVector.elementAt(termIndex));
		return Corpus.corpusVector.elementAt(termIndex);
	}
}
