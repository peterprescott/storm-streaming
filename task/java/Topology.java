package piprescott;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


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
			.shuffleGrouping("positive-bolt");
			// .shuffleGrouping("negative-bolt");
		
		// configure cluster
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setDebug(true);
		cluster.submitTopology("tweet-topology", conf, builder.createTopology());
		Thread.sleep(1000000);
		cluster.shutdown();
	}
}
