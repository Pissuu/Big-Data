package edu.rit.ibd.a5;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class InitializeTransactions {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String sqlQuery = args[3];
		//The sqlQuery should always contain two things: tid and itemid
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
//		final String dbURL = "jdbc:mysql://localhost:3306/imdb_ibd";
//		final String user = "root";
//		final String pwd = "MySQLrox@123";
//		final String sqlQuery = "SELECT mid AS tid, pid AS iid FROM Actor AS a JOIN Movie AS m ON id=mid WHERE year BETWEEN 2008 AND 2012 AND votes > 100000 ORDER BY tid, iid";
//		//The sqlQuery should always contain two things: tid and itemid
//		final String mongoDBURL = "None";
//		final String mongoDBName = "tryout";
//		final String mongoCol = "abcd";
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		// TODO Your code here!!!
		
		/*
		 * 
		 * Run the input SQL query over the input URL. Remember to use the fetch size to only retrieve a certain number of tuples at a time (useCursorFetch=true will
		 * 	be part of the URL).
		 * 
		 * For each transaction (tid), you need to create a new document and store it in the MongoDB collection specified as input. Such document must contain an array
		 * 	in which the elements are iid lexicographically sorted.
		 * 
		 */
		
		// TODO End of your code!

		MongoCollection<Document> col = db.getCollection(mongoCol);	
		col.drop();
//		String sqlQuery1 = sqlQuery + " orderby tid";
		PreparedStatement ps = con.prepareStatement(sqlQuery);
		ps.setFetchSize(25);
		ResultSet rs = ps.executeQuery();
		int tid1 = 0;
		int tid2 = 0;
		List<Integer> items = new ArrayList<>();
		if(rs.next()) {
			if(rs.getString("tid")!=null && rs.getString("iid")!=null) {
				tid2 = rs.getInt("tid");
				items.add(rs.getInt("iid"));
			}
		}
	
		while(rs.next()) {
			//option 1
//			col.insertOne(document);
//			col.updateOne(filter, update);
			//option 2
			if(rs.getString("tid")!=null && rs.getString("iid")!=null) {
				tid1 = rs.getInt("tid");
				if(tid1 == tid2) {
					items.add(rs.getInt("iid"));
				}
				else {
					Document d = new Document();
					Collections.sort(items);
					d.append("_id", tid2);
//					d.append("items", items);
					System.out.println(d);
					col.insertOne(d);
					for(int item: items) {
						col.updateOne(Document.parse("{_id:"+tid2+"}"), Document.parse("{$addToSet : { items:" + item + "}}"));
//						System.out.println(col.find(Document.parse("{_id:"+tid2+"}")).first());
					}
					items.clear();
					tid2 = tid1;
					items.add(rs.getInt("iid"));
				}
			}
		}
		Document d = new Document();
		Collections.sort(items);
		d.append("_id", tid1);
//		d.append("items", items);
		col.insertOne(d);
		for(int item: items)
			col.updateOne(Document.parse("{_id:"+tid1+"}"), 
					Document.parse("{$addToSet : { items:" + item + "}}"));	
		rs.close();
		ps.close();
		client.close();
		con.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
