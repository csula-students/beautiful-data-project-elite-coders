package dataAcquisition;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;

import com.mongodb.ServerAddress;

public class saveData {

	@SuppressWarnings("deprecation")
	public saveData() {
		// TODO Auto-generated constructor stub
		
	}
	
	public void mdb(modelTw u) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		@SuppressWarnings("unused")
		DB db = mongo.getDB("test");
		//DBCollection linked=db.createCollection("twitterData",new BasicDBObject());
		//model data add
		modelTw mdata=u;
		DBObject doc = createDBObject(mdata);
		DBCollection col = db.getCollection("twitterData");
		WriteResult result = col.insert(doc);
		System.out.println("this issssssssss data--------"+result.getUpsertedId());
		 System.out.println(result.getN());
	        System.out.println(result.isUpdateOfExisting());
	       System.out.println(result.unacknowledged());
		
		//read        
		mongo.close();
		
		
	}
	
	 private modelTw createModel() {
		 modelTw u = new modelTw();
	        u.setUser("meo1");
	        u.setComments("hii...whats up");
	        return u;
	    }
	 
	 private static DBObject createDBObject(modelTw mdTw) {
	        BasicDBObjectBuilder docBuilder = BasicDBObjectBuilder.start();
	                                 
	        docBuilder.append("id", mdTw.getUser());
	        docBuilder.append("name", mdTw.getComments());
	       
	        return docBuilder.get();
	    }

}
