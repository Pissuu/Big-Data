package edu.rit.ibd.a5;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BsonField;

public class GenerateLOneOpt {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColTrans = args[2];
		final String mongoColL1 = args[3];
		final int minSup = Integer.valueOf(args[4]);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
//		start _id from 0
//		the mongoColTrans are the transactions that in the form of tid, items and are in ascending order
//		mogocolL1 is the one that we need to generate
		// TODO Your code here!
		
		/*
		 * 
		 * Extract single items from the transactions. Only single items that are present in at least minSup transactions should survive.
		 * 
		 * You need to compose the new documents to be inserted in the L1 collection as {_id:x, items:[iid], count:z}. The _id should be the position of the item
		 * 	id (iid) when they are lexicographically sorted.
		 * 
		 */
		//count should be greater of equal that minSup
		//the _id:x has to be lexicographically sorted

//		MongoCollection<Document> transactions = db.getCollection(mongoColTrans), 
		MongoCollection<Document> transactions = db.getCollection(mongoColTrans);
//				l1 = db.getCollection(mongoColL1);
		MongoCollection<Document> col = db.getCollection(mongoColL1);
		//option 1:Java
//		for(Document t : transactions.find().batchSize(5)) {
//			List<Integer> itemsOfT = t.getList("items", Integer.class);
//		}
		
		//Option 2: Using Mongo DB aggregation queries
//		BsonField group = new BsonField("_id", new Document("_id","$items"));
//		ArrayList<Document> rrr = transactions.aggregate(ImmutableList.of(Aggregates.unwind("$items"),Aggregates.group("_id", group))).into(new ArrayList<>() );
		ArrayList<Document> rrr = transactions.aggregate(ImmutableList.of(Aggregates.unwind("$items"))).into(new ArrayList<>() );
		List<Integer>  r =new ArrayList<Integer>();
		Set<Integer> hs = new HashSet<Integer>();
		for(Document d:rrr) {
			int temp = (int)d.get("items");
			hs.add(temp);
		}
		Hashtable<Integer, Integer> dict = new Hashtable<Integer, Integer>();
		Hashtable<Integer, List<Integer>> dict1 = new Hashtable<Integer, List<Integer>>();
		for(int i: hs) {
			dict.put(i, 0);
			List<Integer> n1 = new ArrayList<Integer>();
			dict1.put(i, n1);
			
		}
		for(Document d:rrr) {
			int item = (int)d.get("items");
			int count = dict.get(item);
			int id = (int)d.get("_id");
			List<Integer> n1 = dict1.get(item);
			n1.add(id);
			dict.put(item, count +1);
			dict1.put(item, n1);
		}
		List<Integer> big=new ArrayList<Integer>();
		for (Integer key : dict.keySet()) {
		    if(dict.get(key)>=minSup)
		    	big.add(key);
		}
		Collections.sort(big);
		int index = 0;
		System.out.println("generateLone");
		for(int i: big) {
			Document d1 = new Document();
			d1.append("_id", index);
			List<Integer> b=new ArrayList<Integer>();
			b.add(i);
			d1.append("items", b);
			d1.append("count", dict.get(i));
			d1.append("transactions", dict1.get(i));
//			System.out.println(d1);
			col.insertOne(d1);
//			System.out.println(col.find(Document.parse("{_id:" + index + "}")).first());
			index = index + 1;
		}
//		for(Document d:rrr)
//			r.add(d.getInteger("items"));
//		Collections.sort(r);
//		List<Integer> traversed=new ArrayList<Integer>();
//		int index = 0;
//		for(int i = 0; i < r.size(); i++) {
//			int item = r.get(i);
//			int count = 0;
//			for(int j = 0; j < r.size(); j++) {
//				if(traversed.contains(j))
//					continue;
//				if (item == r.get(j)) {
//					traversed.add(j);
//					count = count + 1;
//				}
//			}
//			Document d1 = new Document();
//			if(count>=minSup) {
//				d1.append("_id", index );
//				index = index + 1;
//				List<Integer> t=new ArrayList<Integer>();
//				t.add(item);
//				d1.append("items", t);
//				d1.append("count", count);
//				System.out.println(d1);
//				col.insertOne(d1);
//			}
//		}
//		System.out.println(rrr);
		//unwind and then calculate the frequency of occurence
		
		// TODO End of your code!
		
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
