package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.json.JSONObject;

public class Main {

	public static void main(String[] args) throws UnsupportedEncodingException, IOException {
		String line;
		try (
		    InputStream fileInputStream = new FileInputStream("C:\\Users\\Amir\\Desktop\\TinyCorpus.txt");
		    InputStreamReader isr = new InputStreamReader(fileInputStream, "UTF-8");
		    BufferedReader br = new BufferedReader(isr);
		) {
		    while ((line = br.readLine()) != null) {
		    	JSONObject tweetJson = new JSONObject(line);		// Create a new JSON object from a JSON string
		    	System.out.println(tweetJson);
				//tweetJson.getString("text");						// read a field of type 'type' in 'getType'
		    }
		}
		

	}

}
