package edu.rit.ibd.a6;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class InitPoints {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String sqlQuery = args[3];
		final String mongoDBURL = args[4];
		final String mongoDBName = args[5];
		final String mongoCol = args[6];
		
		
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		MongoCollection<Document> col = db.getCollection(mongoCol);
		// TODO Your code here!!!
		
		/*
		 * 
		 * Run the input SQL query over the input URL. Remember to use the fetch size to only retrieve a certain number of tuples at a time 
		 * 	(useCursorFetch=true will be part of the URL).
		 * 
		 * For each point, you need to create a new document and store it in the MongoDB collection specified as input. Such document must contain an array
		 * 	in which the values are the dimensions specified by the query in the appropriate positions. You must assume all of these values are BigDecimal.
		 * 	We will always use scale of 34 digits.
		 * 
		 * Use min-max normalization to normalize the values of the dimensions. When retrieving BigDecimal from MongoDB, the type will be Decimal128; 
		 * 	use the 'bigDecimalValue' method to convert it to BigDecimal.
		 * 
		 */
		
		PreparedStatement ps = con.prepareStatement(sqlQuery);
		ps.setFetchSize(5);
		ResultSet rs = ps.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		int n = rsmd.getColumnCount();
		List<BigDecimal> min = new ArrayList<>();
		List<BigDecimal> max = new ArrayList<>();
		BigDecimal low = new BigDecimal(0);
		BigDecimal high = new BigDecimal(99999);
		for(int i = 1; i < n; i++) {
			min.add(high);
			max.add(low);
		}
		while(rs.next()) {
			Document d = new Document().append("_id", rs.getInt("id"));

			for(int i = 1; i < n; i++) {
				String dim = "dim_" + String.valueOf(i);
				BigDecimal x = rs.getBigDecimal(dim);
				int val = x.compareTo(max.get(i-1));
				if(val == 1)
					max.set(i-1, x);
				val = x.compareTo(min.get(i-1));
				if(val == -1)
					min.set(i-1, x);
			}
		}
//		System.out.println(sqlQuery);
		rs = ps.executeQuery();
		while(rs.next()) {
			List<BigDecimal> points = new ArrayList<>();
			Document d = new Document().append("_id", rs.getInt("id"));
			for(int i = 1; i < n; i++) {
				BigDecimal numerator;
				BigDecimal denominator;
				String dim = "dim_" + String.valueOf(i);
				BigDecimal x = rs.getBigDecimal(dim);
//				BigDecimal min1 = new BigDecimal(min.get(i-1), MathContext.DECIMAL128);
//				BigDecimal max1 = new BigDecimal(max.get(i-1), MathContext.DECIMAL128);
				BigDecimal min1 = min.get(i-1);
				BigDecimal max1 = max.get(i-1);
				numerator = x.subtract(min1, MathContext.DECIMAL128);
				denominator = max1.subtract(min1, MathContext.DECIMAL128);
				BigDecimal bbb = numerator.divide(denominator, MathContext.DECIMAL128);
				
				points.add(bbb);
			}
			d.append("point", points);
			col.insertOne(d);
//			if(rs.getInt("id") == 1478868) {
//				System.out.println(d);
//				System.out.println(min);
//				System.out.println(max);
//			}
				
		}
//		BigDecimal bd = BigDecimal.ZERO;
//		bd = bd.add(BigDecimal.ONE, MathContext.DECIMAL128); // this adds number one to the decimal
//		bd.divide(BigDecimal.ONE, MathContext.DECIMAL128);
		
//		Decimal128 mongoDbDecimal = null;
//		BigDecimal bdFromMongo = mongoDbDecimal.bigDecimalValue();
		 
//		List<Decimal128> point = db.getCollection(mongoCol).find().first().getList("point", Decimal128.class);
		 
//		 you can either
		// TODO End of your code!
		
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
