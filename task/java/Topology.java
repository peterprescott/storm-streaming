package piprescott;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;


public class Topology {
 
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-spout", new TwitterSpout());

		builder.setBolt("filter-bolt", new FilterBolt()).shuffleGrouping("twitter-spout");
		builder.setBolt("negative-bolt", new NegativeBolt()).shuffleGrouping("filter-bolt");
		builder.setBolt("positive-bolt", new PositiveBolt()).shuffleGrouping("negative-bolt");
		builder.setBolt("score-bolt", new ScoreBolt()).shuffleGrouping("positive-bolt");
		
		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		conf.setDebug(true);
		cluster.submitTopology("twitter-direct", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}
}
