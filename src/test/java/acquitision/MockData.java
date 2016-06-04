package acquitision;

/**
 * Mock raw data
 */
public class MockData {
    private String userid;
    private String ratings;
    private String movieid;
    private String ts;
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getRatings() {
		return ratings;
	}
	public void setRatings(String ratings) {
		this.ratings = ratings;
	}
	public String getMovieid() {
		return movieid;
	}
	public void setMovieid(String movieid) {
		this.movieid = movieid;
	}
	public String getTs() {
		return ts;
	}
	public void setTs(String ts) {
		this.ts = ts;
	}
	public MockData(String ratings, String movieid) {
		super();
		this.ratings = ratings;
		this.movieid = movieid;
	}
    
	

   
}
