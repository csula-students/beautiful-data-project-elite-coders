package dataAcquisition;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import au.com.bytecode.opencsv.CSVWriter;
import twitter4j.GeoLocation;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

public class newTwi{
	static String consumerKeyStr = "qnSHMdMT2DnCOybMHkaqwHi7I";
	static String consumerSecretStr = "NQUHN2Hhb6Y1D0n4WftxF3AVhtTZi0TVVUIAjI9auojjMwl0ru";
	static String accessTokenStr = "342627782-X81pmUAPN5o6wFIOYXPrZgKaRP14sxqC8ozqnGJD";
	static String accessTokenSecretStr = "gaUvnFhN2Vombyd0ox0t2qyqz5M1BANak4J1XA5iHd1Nj";
	static ArrayList<String> twitsArr=new ArrayList<String>();	
	 Long lastID = null;
	public Runnable twi() throws InterruptedException, IOException{			 
		 Query query;
		if(lastID==null)
		{
			 query = new Query("#newmovie").since("20140101");	
			 //query.setLang("EN");
		}else{
			 query = new Query("#newmovie").since("20140101").sinceId(lastID);
			 //query.setLang("EN");
			 query.setMaxId(lastID);
		}		
		Twitter twitter = new TwitterFactory().getInstance();
		twitter.setOAuthConsumer(consumerKeyStr, consumerSecretStr);
		AccessToken accessToken = new AccessToken(accessTokenStr,accessTokenSecretStr);
		twitter.setOAuthAccessToken(accessToken);
	    int numberOfTweets = 200;	  
	    ArrayList<Status> tweets = new ArrayList<Status>();
		  while (tweets.size () < numberOfTweets) 
		  {		   
		    try {
		    	
		      QueryResult result = twitter.search(query);
		      lastID=result.getMaxId();
		      tweets.addAll(result.getTweets());
		      
		      System.out.println("Gathered " + tweets.size() + " tweets");	      
		    }
		    catch (TwitterException te) {
		    	System.out.println("Couldn't connect: " + te);
		    	 //Thread.sleep(120000);
		    	Thread.sleep(te.getRateLimitStatus().getSecondsUntilReset() * 1000);	
		    	twi();
		    }; 		   
		  }
		  for (int i = 0; i < tweets.size(); i++) {
			  //Thread.sleep(120000); 	
			 // lastID = tweets.get(0).getId();
		    Status t = (Status) tweets.get(i);
		    GeoLocation loc = t.getGeoLocation();
		    String user = t.getUser().getScreenName();
		    String msg = t.getText();
		    String time = "";
		    //
		    saveData sv=new saveData();
		    //
		    if (loc!=null) {
		      Double lat = t.getGeoLocation().getLatitude();
		      Double lon = t.getGeoLocation().getLongitude();
		      System.out.println(i + " USER: " + user + " wrote: " + msg + " located at " + lat + ", " + lon);		      
		      twitsArr.add(msg);			    
				//modelTw tw=new modelTw(user,msg);
				//sv.mdb(tw);
		      } 
		    else 
		    	System.out.println(i + " USER: " + user + " wrote: " + msg);
		   // modelTw tw=new modelTw(user,msg);
			//sv.mdb(tw);			
		  }
		  query.setMaxId(lastID);
		  twi();
		return null;
	}		
			
		public static void main(String[] args) throws InterruptedException, IOException {		
			try {				
			newTwi tw=new newTwi();
			Thread t=new Thread(tw.twi());
			t.start();			
			} catch (Exception e) {
				
			}	
		}
		
	}


