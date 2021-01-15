// ScoreBolt.java

package piprescott;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.HashMap;
import java.io.PrintWriter;

/**
 * Complete naive sentiment analysis by comparing
 * positive and negative word-counts; tweet is 
 * classified as whichever is greater.
 *
 * @author Pi Prescott
*/

public class ScoreBolt extends BaseBasicBolt {

	private PrintWriter writer;
	private int positiveCount = 0;
	private int negativeCount = 0;
	private String sentiment;

	private HashMap<Integer, Integer> positiveScores = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> negativeScores = new HashMap<Integer, Integer>();

	public void prepare(Map stormConf, TopologyContext context){
		// open file to write simple logs in a CSV that could be analysed further
		String fileName = "logs.csv";
		try {
			this.writer = new PrintWriter(fileName, "UTF-8");
		} catch (Exception e){
			System.out.println("Error: Unable to open logs.csv file");
		}
	}

	public void execute(Tuple input,BasicOutputCollector collector) {

		// sentiment is unknown until scores are compared
		sentiment = "unknown";
		int positiveScore, negativeScore;

		// get incoming inputs
		int tweetId = input.getIntegerByField("tweetId");
		String tweetText = input.getStringByField("tweetText");
		String scoreType = input.getStringByField("scoreType");

		// Positive and negative sentiment scores are computed in parallel,
		// and either could come first, so whichever we receive, we need
		// to check if we have already received the other.
		// If so, we can calculate the sentiment.
		// If not, then we store the score to be found when the other arrives.
		if (scoreType=="positive"){
		    positiveScore = input.getIntegerByField("sentimentScore");
		    if (negativeScores.containsKey(tweetId)){
			    // get negativeScore
			    negativeScore = negativeScores.get(tweetId);
			    // calculate Sentiment
			    int sentimentTotal = positiveScore - negativeScore;
			    if (sentimentTotal>0){
				    sentiment = "Positive";
				    this.positiveCount++;
			    } else if (sentimentTotal<0){
				    sentiment = "Negative";
				    this.negativeCount++;
			    }
			    System.out.println(tweetText + "\n\n" + sentiment);
		    } else {
			    // put positiveScore
			    positiveScores.put(tweetId, positiveScore);
		    }
		} else if (scoreType=="negative"){
		    negativeScore = input.getIntegerByField("sentimentScore");
		    if (positiveScores.containsKey(tweetId)){
			    // get negativeScore
			    positiveScore = positiveScores.get(tweetId);
			    // calculate Sentiment
			    int sentimentTotal = positiveScore - negativeScore;
			    if (sentimentTotal>0){
				    sentiment = "Positive";
				    this.positiveCount++;
			    } else if (sentimentTotal<0){
				    sentiment = "Negative";
				    this.negativeCount++;
			    }
			    System.out.println(tweetText + "\n\n" + sentiment);

		    } else {
			    // put positiveScore
			    negativeScores.put(tweetId, negativeScore);
		    }	    
		}
	
		// Log the results in CSV format
		String logRecord = tweetId + "," + sentiment + ",'" + tweetText + "'\n";
		writer.println(logRecord);

		// Print running record of positive and negative counts
		System.out.println(logRecord);
		System.out.println("Positive:");
		System.out.println(this.positiveCount);
		System.out.println("Negative:");
		System.out.println(this.negativeCount);
	}

    	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void cleanup() {}
}
