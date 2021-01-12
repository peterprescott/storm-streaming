package piprescott;

import org.apache.storm.Config; 
import org.apache.storm.LocalCluster; 
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector; 
import org.apache.storm.task.TopologyContext; 
import org.apache.storm.testing.TestWordSpout; 
import org.apache.storm.topology.OutputFieldsDeclarer; 
import org.apache.storm.topology.TopologyBuilder; 
import org.apache.storm.topology.base.BaseRichBolt; 
import org.apache.storm.tuple.Fields; 
import org.apache.storm.tuple.Tuple; 
import org.apache.storm.tuple.Values; 
import org.apache.storm.utils.Utils; 
import java.util.Map; 

public class Topology { 
	public static class ExclamationBolt extends BaseRichBolt { 
		OutputCollector _collector; 
		
		@Override 
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) { 
			_collector = collector; 
		} 

		@Override 
		public void execute(Tuple tuple) { 
			String s = new String(tuple.getString(0) + "!!!");
			_collector.emit(tuple, new Values(s)); 
			_collector.ack(tuple); 

			// System.out.println( s );
		} 
		
		@Override 
		public void declareOutputFields(OutputFieldsDeclarer declarer) { 
			declarer.declare(new Fields("word")); 
		} 
	} 
	
	public static void main(String[] args) throws Exception { 
		TopologyBuilder builder = new TopologyBuilder(); 
		builder.setSpout("word", new TestWordSpout()); 
		builder.setBolt("exclaim1", new ExclamationBolt()).shuffleGrouping("word"); 
		builder.setBolt("exclaim2", new ExclamationBolt()).shuffleGrouping("exclaim1"); 
		
		Config conf = new Config(); 
		conf.setDebug(true);
		// conf.setNumWorkers(1);
		// conf.setMaxSpoutPending(5000);
		StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
	} 
}
