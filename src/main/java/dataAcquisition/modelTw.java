package dataAcquisition;

public class modelTw {

	public modelTw() {
		// TODO Auto-generated constructor stub
		
	}
	private String user;
	private String comments;
	public String getUser() {
		return user;
	}
	public modelTw(String user, String comments) {
		super();
		this.user = user;
		this.comments = comments;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getComments() {
		return comments;
	}
	public void setComments(String comments) {
		this.comments = comments;
	}

}
