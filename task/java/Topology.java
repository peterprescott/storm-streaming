// Topology.java

package piprescott;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.generated.StormTopology;

/**
 * Main Topology class to set up Storm topology for COMP518 assignment.
 *
 * @author PI Prescott
*/

public class Topology {

	public static void main(String[] args) throws InterruptedException {

		// build topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-spout", new TwitterSpout());
		builder.setBolt("filter-bolt", new FilterBolt())
			.shuffleGrouping("twitter-spout");
		builder.setBolt("positive-bolt", new PositiveBolt())
			.shuffleGrouping("filter-bolt");
		builder.setBolt("negative-bolt", new NegativeBolt())
			.shuffleGrouping("filter-bolt");
		builder.setBolt("score-bolt", new ScoreBolt())
			.fieldsGrouping("positive-bolt", new Fields("tweetId"))
			.fieldsGrouping("negative-bolt", new Fields("tweetId"));
		
		// configure...
		Config conf = new Config();
		conf.setDebug(true);
		StormTopology topology = builder.createTopology();

		// ...comment/uncomment depending on whether running local/distributed...
			// configure local cluster
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("local-tweet-topology", conf, topology);
			Thread.sleep(1000000);
			cluster.shutdown();
		
// 			// configure distributed cluster
// 			conf.setNumWorkers(3);
// 			conf.setMaxSpoutPending(5000);
// 			StormSubmitter.submitTopology("tweet-topology", conf, topology);
	}
}
