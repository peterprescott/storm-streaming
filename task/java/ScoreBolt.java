package piprescott;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class ScoreBolt extends BaseBasicBolt {

	private int positiveCount = 0;
	private int negativeCount = 0;
	private int neutralCount = 0;

	public void execute(Tuple input,BasicOutputCollector collector) {
		

		String tweetText = input.getStringByField("tweetText");
		// String negtweetText = input.getStringByField("negative-tweetText");
		// String filteredText = input.getStringByField("negativeWords");
		int positiveScore = input.getIntegerByField("positiveScore");
		int negativeScore = 0;
		// int negativeScore = input.getIntegerByField("negativeScore");
		String sentiment = "unknown";
		if (positiveScore > negativeScore){ 
			sentiment = "Positive";
			this.positiveCount++;
		} else if (negativeScore > positiveScore){
			sentiment = "Negative";
			this.negativeCount++;
		} else {
			sentiment = "Neutral";
			this.neutralCount++;
		}

		System.out.println(tweetText);
		// System.out.println(negtweetText);
		// System.out.println(filteredText);
		System.out.println(sentiment);
		System.out.println("\nPositives:");
		System.out.println(this.positiveCount);
		System.out.println("\nNegatives:");
		System.out.println(this.negativeCount);
	
		collector.emit(new Values(sentiment));



	}

    	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	public void cleanup() {}

}
