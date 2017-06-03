package corpusData;

public class Word {
	
	
	
	
	// tf - term frequency (number of times the term occurs in the corpus)
	// augmented frequency, to prevent a bias towards longer documents,
	// e.g. raw frequency divided by the maximum raw frequency of any term in the document
	
	public int tf () {
		
	}
	
	
	// idf - inverse document frequency (whether the term is common or rare across all documents)
	// It is the logarithmically scaled inverse fraction of the documents that contain the word, 
	// obtained by dividing the total number of documents by the number of documents containing the term, 
	// and then taking the logarithm of that quotient
	
	public int idf () {
		
	}
	
	
	// tf-idf is basically the frequency of a term (word) multiplied by its inverse document frequency (to reduce the value 
	// of non-meaningful words that appear a lot). It reflects the importance of a term in a tweet. 
	// Since tweets contain no more than 140 characters, most of the vector will contain zeros; 
	// therefore, we'll save/use the vectors in a non-sparse form, where we save only indices and values that are not zero.
	
	public int tf_idf () {
		
	}
	
	
	
	public boolean isStopWord(String term) {
		
	}
}
