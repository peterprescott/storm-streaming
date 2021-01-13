package piprescott;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.*;


public class FilterBolt extends BaseBasicBolt {

	// list of irrelevant words to be filtered out.
	private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
            "http", "https", "the", "you", "que", "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", 
            "about", "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would",
	}));

	public void cleanup() {}

	public void execute(Tuple input,BasicOutputCollector collector) {

		Status tweet = (Status) input.getValueByField("tweet");
		String tweetText = tweet.getText();

		System.out.println(tweetText);

		String text = tweetText.replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
		String[] words = text.split(" ");
		String[] filteredWords = words.removeIf(w -> IGNORE_LIST.contains(w));

		collector.emit(new Values(tweetText, filteredWords));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet-text","filtered-words"));
	}

}
