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

public class TwitterSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    //Queue for tweets
    private LinkedBlockingQueue<Status> queue;
    //stream of tweets
    private TwitterStream twitterStream;

    // open spout
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;

	// configure Twitter API access
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("tpAestpXtM2pYAlomfZr6LN7d")
                .setOAuthConsumerSecret("MCQ1aVPypaBOZIlg7MDp36znULAIcmf9Cj8xfxodyVyLpILpQu")
                .setOAuthAccessToken("124163864-koQiHbqAF1QvLUzGqMb2ITvWk60jaa5yOsgJeaT7")
                .setOAuthAccessTokenSecret("qdMSjnab0O49k1pnck0fgtVQre60VN7pb0qkSC2vSYwJE");


	// initiate Twitter stream
        this.twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        this.queue = new LinkedBlockingQueue<Status>();

	// we are only interested in the status message of the incoming tweets
        final StatusListener listener = new StatusListener() {
            public void onStatus(Status status) { queue.offer(status); }
            public void onDeletionNotice(StatusDeletionNotice sdn) {}
            public void onTrackLimitationNotice(int i) {}
            public void onScrubGeo(long l, long l1) {}
            public void onException(Exception e) {}
            public void onStallWarning(StallWarning warning) {}
        };
        twitterStream.addListener(listener);

	// we track hashtags related to the COVID19 vaccine
        final FilterQuery query = new FilterQuery();
        query.track(new String[]{"#COVID19","Vaccine","COVIDVaccine"});
        twitterStream.filter(query);
    }

    public void nextTuple() {
	// emit tweet text from the queue of incoming Twitter status messages
        final Status status = queue.poll();
        if (status == null) {
            Utils.sleep(50);
        } else {
            collector.emit(new Values(status));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	// declare 'tweet'
        declarer.declare(new Fields("tweet"));
    }

    public void close() {
	// shutdown Twitter stream when closing spout
        twitterStream.shutdown();
    }


}
