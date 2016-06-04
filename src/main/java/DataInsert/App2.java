package dataInsert;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.aggregations.support.format.ValueFormat.DateTime;

import java.io.File;
import java.io.IOException;
import java.net.StandardSocketOptions;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
//data insert
/**
 * Quiz elastic search app to see Salaries.csv file better
 *
 * gradle command to run this app `gradle esQuiz`
 *
 * Before you send data, please run the following to update mapping first:
 *
 * ```
 PUT /movies-data
 {
     "mappings" : {
         "movies" : {
             "properties" : {
                 "userId" : {
                     "type" : "long",
                     "index" : "not_analyzed"
                 },
                 "movieId" : {
                     "type" : "integer",
                     "index" : "not_analyzed"
                 },
                 "ratings" : {
                     "type" : "double",
                     "index" : "not_analyzed"
                 },
                 "year": {
                     "type": "integer"
                 }
                 
                 "title": {
                     "type": "string"
                 }
                 
                 "genres": {
                     "type": "string"
                 }
             }
         }
     }
 }
 ```
 */
public class App2 {
    private final static String indexName = "movies-data-info";
    private final static String typeName = "moviesinfo";

    public static void main(String[] args) throws URISyntaxException, IOException {
        Node node = nodeBuilder().settings(Settings.builder()
            .put("real-rakkesh", "your-namex`")
            .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        /**
         *
         *
         * INSERT data to elastic search
         */

        // as usual process to connect to data source, we will need to set up
        // node and client// to read CSV file from the resource folder
        File csv = new File(
            ClassLoader.getSystemResource("Usermovies.csv")
                .toURI()
        );

        // create bulk processor
        BulkProcessor bulkProcessor = BulkProcessor.builder(
            client,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId,
                                       BulkRequest request) {
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      Throwable failure) {
                    System.out.println("Facing error while importing data to elastic search");
                    failure.printStackTrace();
                }
            })
            .setBulkActions(10000)
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
            .build();

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        try {
            // after reading the csv file, we will use CSVParser to parse through
            // the csv files
            CSVParser parser = CSVParser.parse(
                csv,
                Charset.defaultCharset(),
                CSVFormat.EXCEL.withHeader()
            );

           
            // for each record, we will insert data into Elastic Search
            parser.forEach(record -> {
            	try{
            	String dte=record.get("timestamp");
            	
            	@SuppressWarnings("deprecation")
				Date now = new Date(Long.parseLong(dte));
            	DateFormat format = new SimpleDateFormat("MMddyyHHmmss");
            	//Date date = null;
            	//
            	long unixSeconds = Long.parseLong(dte);
        		Date date = new Date(unixSeconds*1000L); // *1000 is to convert seconds to milliseconds
        		SimpleDateFormat sdf = new SimpleDateFormat("yyyy"); // the format of your date
        		//sdf.setTimeZone(TimeZone.getTimeZone("GMT-4")); // give a timezone reference for formating (see comment at the bottom
        		String formattedDate = sdf.format(date);
        		System.out.println("---"+formattedDate);
            	//
            	@SuppressWarnings("deprecation")
				int newdt=now.getYear()+1900;
            	//
            	String[] genresArray=record.get("genres").split("\\|");
            	System.out.println(genresArray[0]);
            	//
            	//for (int i = 0; i < args.length-1; i++) {			
            	
                Salary salary = new Salary(
                    Long.parseLong(record.get("userId")),
                    Integer.parseInt(record.get("movieId")),
                   Double.parseDouble(record.get("rating")),
                  Integer.parseInt(formattedDate),record.get("title"),genresArray[0]  
                    );
            	
                //
                bulkProcessor.add(new IndexRequest(indexName, typeName)
                    .source(gson.toJson(salary))
                );
                
            	//}
            }catch(Exception e){
            	System.out.println(e);
            }
            }
            );
            
        } catch (IOException e) {
            e.printStackTrace();
        }
           
    }

    private static Double parseSafe(String value) {
        return Double.parseDouble(value.isEmpty() || value.equals("Not Provided") ? "0" : value);
    }

    static class Salary {
        private final long userId;
        private final int movieId;        
        private final double ratings;
        private final int year;
        private final String title;
        private final String genres;
        
        public Salary(long userid, int movieid, double ratings, int year,String title,String genres) {
            this.userId = userid;
            this.movieId = movieid;
            this.ratings = ratings;
            this.year =year;
            this.title=title;
            this.genres=genres;
            }

		

		





public long getUserid() {
			return userId;
		}

		public int getMovieid() {
			return movieId;
		}

		public double getRatings() {
			return ratings;
		}

		public int getYear() {
			return year;
		}
		public String gettitle() {
			return title;
		}
		public String getgenres() {
			return genres;
		}
       
    }
}
