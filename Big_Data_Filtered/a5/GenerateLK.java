package edu.rit.ibd.a5;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class GenerateLK {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColCK = args[3];
		final String mongoColLK = args[4];
		final int minSup = Integer.valueOf(args[5]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		// TODO Your code here!
		
		/*
		 * 
		 * For each transaction t, check whether the items of a document c in ck are contained in the items of t. If so, increment one the count of c.
		 * 
		 * All the documents in ck that meet the minimum support will be copied to lk.
		 * 
		 * You can use $inc to update the count of a document.
		 * 
		 * Alternatively, you can also copy all documents in ck to lk first and, then, perform the previous computations.
		 * 
		 */
		
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
		MongoCollection<Document> ck = db.getCollection(mongoColCK);
		MongoCollection<Document> col = db.getCollection(mongoColLK);
		int index= 0;

		for(Document cand: ck.find().batchSize(25)) {
			int id = (int) cand.get("_id");
//			System.out.println("are we ever goning to come here");
			List<Integer>list1 = new ArrayList<Integer>();
			list1 = cand.getList("items", Integer.class);
			int count = 0;
			for(Document t: transactions.find().batchSize(25)) {
				List<Integer>list2 = new ArrayList<Integer>();
				list2 = t.getList("items", Integer.class);
				if(list2.containsAll(list1)) {
					count = count + 1;
				}
				else {
//					System.out.println("list " + list2 + "does not contain all of :" + list1);
				}
				//cand is included in t: +1 else do nothing
				//update either ck or lk
			}
			if(count>=minSup) {
				Document d = new Document();
				d.append("_id",id );
				index = index + 1;
//				System.out.println(index);
				d.append("items", list1);
				d.append("count", count);
//				System.out.println(d);
				col.insertOne(d);
			}
				
		}
		//if you are updating ck -> load lk with those that meet the minsup
		// if you are updating lk -> remove from lk those that do not meet the minsup
		
		
		// TODO End of your code here!
		
		client.close();
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
