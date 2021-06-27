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

public class AprioriGen {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColLKMinusOne = args[2];
		final String mongoColCK = args[3];
		
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
		MongoCollection<Document> col = db.getCollection(mongoColCK);
		int kMinusOne = 0;//lkMinusOne.find().// Get the number of items.
		int count = 0;
		int index1 = 0;
		for(Document j : lkMinusOne.find().batchSize(5)) {
			index1 = index1 + 1;
			List<Integer> list1 = new ArrayList<Integer>();
			list1 = (List<Integer>) j.get("items");
			int index2 = 0;
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
//							System.out.println("1 is: " + list1);
//							System.out.println("2 is: " + list2);
//							System.out.println("it has not matched at " + list1.get(i) +"   "+ list2.get(i));
							flag = 1;	
							break;
						}					
					}
					if(flag == 0 && list1.get(list2.size()-1)<list2.get(list2.size()-1)) {
						
						List<Integer>list3 = new ArrayList<Integer>();
						list3.addAll(list1);
						list3.add(list2.get(list2.size()-1));
//						System.out.println(list3);
						d.append("_id", count);
						col.insertOne(d);
						d.append("items", list3);
						d.append("count", 0);
						for(int item: list3)
							col.updateOne(Document.parse("{_id:" + count + "}"), 
									Document.parse("{$addToSet : { items:" + item + "}}"));
						col.updateOne(Document.parse("{_id:" + count + "}"), Document.parse("{$set : { count:" + 0 + "}}"));
						count = count + 1;
//						System.out.println(d);
					}
					else
						break;
				}
				else {
					List<Integer>list3 = new ArrayList<Integer>();
					if(list1.get(0)<list2.get(0)) {
						list3.addAll(list1);
						list3.addAll(list2);
	//					System.out.println(list3);
						d.append("_id", count);
						col.insertOne(d);
						d.append("items", list3);
						d.append("count", 0);
						for(int item: list3)
							col.updateOne(Document.parse("{_id:" + count + "}"), 
									Document.parse("{$addToSet : { items:" + item + "}}"));	
						col.updateOne(Document.parse("{_id:" + count + "}"), Document.parse("{$set: { count:" + 0 + "}}"));
						System.out.println(d);
						count = count + 1;
					}
				}
			}
			}
		}
//		for(Document l : ck.find().batchSize(5)) {
//			System.out.println(l.get("_id"));
//		}
	
		
		//Join step
//		for(Document p: lkMinusOne.find().batchSize(5)) {
//			for(Document q: lkMinusOne.find().batchSize(5)) {
//			//Join p and q.
//			Document join = new Document();
//			//You need to take of _id
//			
//			ck.insertOne(join);
//			}
//		}

		// prune step
		
//	for(Document cand : ck.find().batchSize(5)) {
//		for( Set<Integer> comb : Sets.combinations(new HashSet<>(cand.getList("items", Integer.class)), kMinusOne)) {
//			//comb is present in lk Minus One.
//			comb.toString();
//		}
//	}
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
