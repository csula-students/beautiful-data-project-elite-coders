package twitterapi;

import com.google.common.collect.Lists;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Collection;
import java.util.List;

/**
 * An example of Source implementation using Twitter4j api to grab tweets
 */
public class TwitterSource implements Source<Status> {
    private long minId;
    private final String searchQuery;

    public TwitterSource(long minId, String query) {
        this.minId = minId;
        this.searchQuery = query;
    }

    @Override
    public boolean hasNext() {
        return minId > 0;
    }

    @Override
    public Collection<Status> next() {
        List<Status> list = Lists.newArrayList();
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
            .setOAuthConsumerKey("qnSHMdMT2DnCOybMHkaqwHi7I")
            .setOAuthConsumerSecret("NQUHN2Hhb6Y1D0n4WftxF3AVhtTZi0TVVUIAjI9auojjMwl0ru")
            .setOAuthAccessToken("342627782-X81pmUAPN5o6wFIOYXPrZgKaRP14sxqC8ozqnGJD")
            .setOAuthAccessTokenSecret("gaUvnFhN2Vombyd0ox0t2qyqz5M1BANak4J1XA5iHd1Nj");
        
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();
        //Twitter twitter = tf.getInstance();

        Query query = new Query(searchQuery);
        //query.setLang("EN");
       query.setSince("2016-05-11");
       query.setUntil("2016-05-14");
        if (minId != Long.MAX_VALUE) {
            query.setMaxId(minId);
        }
        list.addAll(getTweets(twitter, query));

        return list;
    }

    private List<Status> getTweets(Twitter twitter, Query query) {
        QueryResult result;
        List<Status> list = Lists.newArrayList();
        try {
        	result = twitter.search(query);
        	 List<Status> tweets = result.getTweets();
           int numberOfTweets=100;
			// do {
        	while(99 < numberOfTweets){  
               
                System.out.println("Gathered " + tweets.size() + " tweets");
                
                for (Status tweet : tweets) {
                    minId = Math.min(minId, tweet.getId());
                }
                list.addAll(tweets);
                numberOfTweets-=1;
            } //while ((query = result.nextQuery()) != null);
        } catch (TwitterException e) {
            // Catch exception to handle rate limit and retry
            e.printStackTrace();
            System.out.println("Got twitter exception. Current min id " + minId);
            try {
                Thread.sleep(e.getRateLimitStatus()
                    .getSecondsUntilReset() * 1000);
                list.addAll(getTweets(twitter, query));
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }

        return list;
    }
}
