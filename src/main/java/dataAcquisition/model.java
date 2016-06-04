package dataAcquisition;

public class model {
	
	String id;
	String movid;
	String rating;
	String ts;
	
	public model(String id,String movid,String rating,String ts)
	{
		super();
		this.id = id;
		this.movid = movid;
		this.rating = rating;
		this.ts = ts;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getMovid() {
		return movid;
	}

	public void setMovid(String movid) {
		this.movid = movid;
	}

	public String getRating() {
		return rating;
	}

	public void setRating(String rating) {
		this.rating = rating;
	}

	public String getTs() {
		return ts;
	}

	public void setTs(String ts) {
		this.ts = ts;
	}
	

}
