package piprescott;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class Topology {
 
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();

		// spout and bolts
		builder.setSpout("tweets-collector", new TwitterSpout());
		builder.setBolt("filter-bolt", new FilterBolt()).
			shuffleGrouping("tweets-collector"); 
		
		// cluster deployment
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setDebug(true);
		cluster.submitTopology("twitter-direct", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}
}
