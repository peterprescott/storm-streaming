package piprescott;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;


public class extractStatusBolt extends BaseBasicBolt {



	public void cleanup() {
		
	}


	public void execute(Tuple input,BasicOutputCollector collector) {


		Status status = (Status) input.getValueByField("tweet");
		    String tweetText = status.getText();

		    System.out.println(tweetText);

			collector.emit(new Values(tweetText));

			}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("tweet"));
	}



}
