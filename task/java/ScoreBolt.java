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

public class ScoreBolt extends BaseBasicBolt {

	private PrintWriter writer;
	private int positiveCount = 0;
	private int negativeCount = 0;
	private int neutralCount = 0;
	private String sentiment;

	private HashMap<Integer, Integer> positiveScores = new HashMap<Integer, Integer>();
	private HashMap<Integer, Integer> negativeScores = new HashMap<Integer, Integer>();

	public void prepare(Map stormConf, TopologyContext context){

		System.out.println(stormConf.get("dirToWrite").toString());
		String fileName = "logs.txt";
		try {
			this.writer = new PrintWriter(fileName, "UTF-8");
		} catch (Exception e){}
	}

	public void execute(Tuple input,BasicOutputCollector collector) {

		sentiment = "unknown";
		int positiveScore, negativeScore;

		int tweetId = input.getIntegerByField("tweetId");
		String tweetText = input.getStringByField("tweetText");
		String scoreType = input.getStringByField("scoreType");

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

	
		String logRecord = tweetId + "," + sentiment + "," + tweetText + "\n";

		System.out.println(logRecord);
		System.out.println("Positive:");
		System.out.println(this.positiveCount);
		System.out.println("Negative:");
		System.out.println(this.negativeCount);

		writer.println(logRecord);
			
	}

    	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void cleanup() {}

}
