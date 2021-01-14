package piprescott;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class Topology {
 
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-spout", new TwitterSpout());
		builder.setBolt("filter-bolt", new FilterBolt()).
			shuffleGrouping("twitter-spout"); 
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setDebug(true);
		cluster.submitTopology("tweet-topology", conf, builder.createTopology());
		Thread.sleep(10000);
	cluster.shutdown();
	}
}
