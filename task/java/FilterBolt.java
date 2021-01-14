package piprescott;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import twitter4j.Status;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


public class FilterBolt extends BaseBasicBolt {

	private Set<String> irrelevantWords = new HashSet<String>(Arrays.asList(new String[] {
		"http", "https", "the", "you", "que", "and", "for", "that", 
		"like", "have", "this", "just", "with", "all", "get", "about", 
		"can", "was", "not", "your", "but", "are", "one", "what", 
		"out", "when", "get", "lol", "now", "para", "por", "want", 
		"will", "know", "good", "from", "las", "don", "people", "got", 
		"why", "con", "time", "would", "is", "at", "football"
	    }));

	public void execute(Tuple input,BasicOutputCollector collector) {

		// First convert tweet text to array of words
		Status status = (Status) input.getValueByField("tweet");
		String tweetText = status.getText();

		String text = tweetText
				.replaceAll("\\p{Punct}", "")
				.replaceAll("\\r|\\n", "")
				.toLowerCase();

		String[] words = text.split(" ");

		// first we create an extensible ArrayList to add filtered words to
		ArrayList<String> filteredWords = new ArrayList<String>();

		for (String word: words){
			if (!irrelevantWords.contains(word)){
				filteredWords.add(word);
			}
		}

		String filteredText = filteredWords.toString();

		// System.out.println(tweetText);

		collector.emit(new Values(tweetText, filteredText));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("tweetText", "filteredText"));
	}

	public void cleanup() {	}

}
