package edu.rit.ibd.a5;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class AprioriGenOpt {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColLK = args[3];
		final int minSup = Integer.valueOf(args[4]);
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		// TODO Your code here!
		
		/*
		 * 
		 * First, you must figure out the current k-1 by checking the number of items in the input collection.
		 * 
		 * Then, you must start two pointers p and q such that p.items[0]==q.items[0] AND p.items[1]==q.items[1] AND ... AND p.items[k-2]==q.items[k-2]. Furthermore,
		 * 	p.items[k-1]<q.items[k-1].
		 * 
		 * If the previous condition is true, a new document d as follows is candidate to be added:
		 * 		p.items[0], p.items[1], p.items[k-2], p.items[k-1], q.items[k-1]
		 * 
		 * Before adding it, we must check that all its subsets of size (k-1) were present in Lk-1. Use Sets.combinations to get each of these subsets. If for a given subset
		 * 	s, there is no document that contains s, the previous document d is pruned.
		 * 
		 * Otherwise, d is added to Ck. Note that the _id of d must be the position of the document when the documents are sorted lexicographically based on the items.
		 * 
		 */

		MongoCollection<Document> lkMinusOne = db.getCollection(mongoColLKMinusOne);
//				ck = db.getCollection(mongoColCK);
		MongoCollection<Document> col = db.getCollection(mongoColLK);
//		col.drop();
		int kMinusOne = 0;//lkMinusOne.find().// Get the number of items.
		int count = 0;
		int index1 = 0;
		for(Document j : lkMinusOne.find().batchSize(5)) {
			index1 = index1 + 1;
			List<Integer> list1 = new ArrayList<Integer>();
			list1 = (List<Integer>) j.get("items");
			int index2 = 0;
//			int id = j.get("_id", Integer.class);
//			for(Document k : lkMinusOne.find().skip(id).batchSize(5)) {
			for(Document k : lkMinusOne.find().batchSize(5)) {
				index2 = index2 + 1;
				if(index2>index1) {
				Document d = new Document();
				List<Integer> list2 = new ArrayList<Integer>();
				list2 = (List<Integer>) k.get("items");
				if(list1.size()>1) {
					int flag = 0;
					for(int i=0; i<list1.size()-1; i++) {
						if(!list1.get(i).equals(list2.get(i))) {
							flag = 1;	
							break;
						}					
					}
					if(flag == 0 && list1.get(list2.size()-1)<list2.get(list2.size()-1)) {
						
						List<Integer>list3 = new ArrayList<Integer>();
						List<Integer>l = new ArrayList<Integer>();
						list3.addAll(list1);
						list3.add(list2.get(list2.size()-1));
						d.append("_id", count);
						List <Integer> intersect = j.getList("transactions", Integer.class);
						List<Integer>l3 = new ArrayList<Integer>();
						for(int i:intersect)
							l3.add(i);
						l3.retainAll(k.getList("transactions", Integer.class));
						if(l3.size()>=minSup) {
							col.insertOne(d);
//							System.out.println("the items are the combo of " + list1 + list2 + " which is " + list3);
						for(int item: list3)
							col.updateOne(Document.parse("{_id:" + count + "}"), 
									Document.parse("{$addToSet : { items:" + item + "}}"));
						for(int trans: l3)
							col.updateOne(Document.parse("{_id:" + count + "}"), 
									Document.parse("{$addToSet : { transactions:" + trans + "}}"));
						col.updateOne(Document.parse("{_id:" + count + "}"), Document.parse("{$set : { count:" + l3.size() + "}}"));
						count = count + 1;
						System.out.println(col.find(Document.parse("{_id:" + count + "}")).first());
						}
					}
					else
						break;
				}
				else {
					if(list1.get(0)>=list2.get(0))
						continue;
					List<Integer>list3 = new ArrayList<Integer>();
					List <Integer> r1 = j.getList("items", Integer.class);
					List <Integer> r2 = k.getList("items", Integer.class);
					if(list1.get(0)<list2.get(0)) {
						list3.addAll(list1);
						list3.addAll(list2);
						d.append("_id", count);
						
						List <Integer> intersect = j.getList("transactions", Integer.class);
						List<Integer>l4 = new ArrayList<Integer>();
						for(int i:intersect)
							l4.add(i);
						l4.retainAll(k.getList("transactions", Integer.class));
						if(l4.size()>=minSup) {
							col.insertOne(d);
						for(int item: list3)
							col.updateOne(Document.parse("{_id:" + count + "}"), 
									Document.parse("{$addToSet : { items:" + item + "}}"));	
						for(int trans: l4)
							col.updateOne(Document.parse("{_id:" + count + "}"), 
									Document.parse("{$addToSet : { transactions:" + trans + "}}"));
						col.updateOne(Document.parse("{_id:" + count + "}"), Document.parse("{$set: { count:" + l4.size() + "}}"));
						System.out.println(col.find(Document.parse("{_id:" + count + "}")).first());
						count = count + 1;
					}
					}
				}
			}
			}
		}
	
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
