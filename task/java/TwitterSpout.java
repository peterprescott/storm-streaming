// TwitterSpout.java

package piprescott;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Reads Twitter stream using the twitter4j library. Set to track COVID19 hashtag and keywords.
 *
 * @author Pi Prescott.
*/

public class TwitterSpout extends BaseRichSpout {

	// Queue for tweets
	private LinkedBlockingQueue<Status> queue;
    	
	// stream of tweets
    	private TwitterStream twitterStream;

	// standard Storm Spout output collector
    	private SpoutOutputCollector collector;
    
	// open Spout
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {

		this.collector = collector;

		// configure Twitter API connection
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
			.setOAuthConsumerKey("xxxxx")
			.setOAuthConsumerSecret("xxxxx")
			.setOAuthAccessToken("xxxxx")
			.setOAuthAccessTokenSecret("xxxxx");

		this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		this.queue = new LinkedBlockingQueue<Status>();

		// listen for new Twitter status messages
		final StatusListener listener = new StatusListener() {
		    public void onStatus(Status status) {
			    // add tweets to queue
			    queue.offer(status);
		    }
		    public void onDeletionNotice(StatusDeletionNotice sdn) {}
		    public void onTrackLimitationNotice(int i) {}
		    public void onScrubGeo(long l, long l1) {}
		    public void onException(Exception e) {}
		    public void onStallWarning(StallWarning warning) {}
		};

		twitterStream.addListener(listener);

		// track tweets related to coronavirus
		final FilterQuery query = new FilterQuery();
		query.track(new String[]{"#COVID19","Vaccine","COVIDVaccine"});
		twitterStream.filter(query);
	    }

	    public void nextTuple() {

		final Status status = queue.poll();

		if (status == null) {
			// wait if necessary
		    	Utils.sleep(50);
		} else {
			// emit tweets as they come
			collector.emit(new Values(status));
		}
	    }

	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	    }

	    public void close() {
		twitterStream.shutdown();
	    }
	}
