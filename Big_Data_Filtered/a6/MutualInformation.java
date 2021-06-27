package edu.rit.ibd.a6;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MutualInformation {

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColElements = args[2];
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		MongoCollection<Document> col = db.getCollection(mongoColElements);

		
//		U = {U_1 = {x_1, x_3, x_5}, U_2 = {x_2, x_4}, U_3 = {x_6}}
//		V = {V_1 = { x_3, x_5}, V_2 = {x_1, x_4}, V_3 = {x_2}, V_4 = {x_6}}
		
//		x_1, u = 1, v = 2;
//		x_2, u = 2, v = 3;
//		.....
//		x_6 , 
		
//		_id = u_1, current_prob = 3/6, other_prob_1 = 2/6, other_prob_2 = 1/6, other_prob_3 = 0/6, other_prob_4 = 0/6
//		...
//		_id = v_j, current_prob = 2/6, other_prob_1 = ...
		
//		
		HashMap<Integer,List<Integer>> u = new HashMap<Integer, List<Integer>>();
		HashMap<Integer,List<Integer>> v = new HashMap<Integer, List<Integer>>();
		int total = 0;
		for(Document j : col.find().batchSize(5)) {
			total = total + 1;
			List<Integer> temp1 = new ArrayList<Integer>();
			u.put(j.getInteger("u"), temp1);
			List<Integer> temp2 = new ArrayList<Integer>();
			v.put(j.getInteger("v"), temp2);
		}
//		System.out.println(u.get(64730));
		for(Document j : col.find().batchSize(5)) {
			int u_id = j.getInteger("u");
			int v_id = j.getInteger("v");
			List<Integer> u_temp = new ArrayList<Integer>();
//			System.out.println(u_id + " " + u.get(u_id));
			u_temp = u.get(u_id);
			u_temp.add(u_id);
			List<Integer> v_temp = new ArrayList<Integer>();
			v_temp = v.get(v_id);
			v_temp.add(v_id);
			u.put(j.getInteger("u"),u_temp);
			v.put(j.getInteger("v"), v_temp);
		}
		col.drop();

//		Set keys = u.keySet();
//		Iterator u_keys = keys.iterator();
//		while(u_keys.hasNext()) {
//			int i = (int) u_keys.next();
//			Document d = new Document();
//			String index = "u_" + String.valueOf(i);
//			d.append("_id", index);
//			col.insertOne(d);
//		}
//		for(Document k: col.find().batchSize(5)) {
//			System.out.println(k);
//		}
		Set keys1 = u.keySet();
		Iterator u_keys1 = keys1.iterator();
		while(u_keys1.hasNext()) {
			Document d = new Document();
			Integer key = (Integer) u_keys1.next();
			List<Integer> temp = new ArrayList<Integer>();
			temp = u.get(key);
			BigDecimal prob = new BigDecimal(0);
			prob = new BigDecimal(temp.size());
			prob = prob.divide(new BigDecimal(total), MathContext.DECIMAL128);
//			Document x = new Document().append("$set", new Document().append("current_prob", prob));
			String index = "u_" + String.valueOf(key);
			d.append("_id", index);
			d.append("current_prob", prob);
//			col.updateOne(Document.parse("{_id:" + index + "}"), x);
			Set keys2 = v.keySet();
			Iterator v_keys = keys2.iterator();
			while(v_keys.hasNext()) {
				Integer v_key = (Integer) v_keys.next();
				List<Integer> temp1 = new ArrayList<Integer>();
				temp1 = v.get(v_key);
				int matches = 0;
				for(int h = 0; h< temp.size(); h++) {
					for(int l = 0; l < temp1.size(); l++) {
						if(temp.get(h).equals(temp1.get(l)))
							matches = matches + 1;}}
				BigDecimal other_prob = new BigDecimal(matches);
				other_prob = other_prob.divide(new BigDecimal(total), MathContext.DECIMAL128);
				String other_name = "other_prob_" + String.valueOf(v_key);
				Document y = new Document().append("$set", new Document().append(other_name, prob));
				d.append(other_name, other_prob);
//				col.updateOne(Document.parse("{_id:" + index + "}"), y);
			}
			col.insertOne(d);
		}
		keys1 = v.keySet();
		Iterator v_keys1 = keys1.iterator();
		while(v_keys1.hasNext()) {
			Document d = new Document();
			Integer key = (Integer) v_keys1.next();
			List<Integer> temp = new ArrayList<Integer>();
			temp = v.get(key);
			BigDecimal prob = new BigDecimal(0);
			prob = new BigDecimal(temp.size());
			prob = prob.divide(new BigDecimal(total), MathContext.DECIMAL128);
//			Document x = new Document().append("$set", new Document().append("current_prob_", prob));
			String index = "v_" + String.valueOf(key);
//			col.updateOne(Document.parse("{_id:" + index + "}"), x);
			d.append("_id", index);
			d.append("current_prob", prob);
			Set keys2 = u.keySet();
			Iterator u_keys = keys2.iterator();
			while(u_keys.hasNext()) {
				Integer u_key = (Integer) u_keys.next();
				List<Integer> temp1 = new ArrayList<Integer>();
				temp1 = u.get(u_key);
				int matches = 0;
				for(int h = 0; h< temp.size(); h++) {
					for(int l = 0; l < temp1.size(); l++) {
						if(temp.get(h).equals(temp1.get(l)))
							matches = matches + 1;}}
				BigDecimal other_prob = new BigDecimal(matches);
				other_prob = other_prob.divide(new BigDecimal(total), MathContext.DECIMAL128);
				String other_name = "other_prob_" + String.valueOf(u_key);
				d.append(other_name, other_prob);
//				Document y = new Document().append("$set", new Document().append(other_name, prob));
//				col.updateOne(Document.parse("{_id:" + index + "}"), y);
			}
			col.insertOne(d);
		}

		for(Document k: col.find().batchSize(5)) {
			System.out.println(k);
		}
		// TODO Your code here!
		
		/*
		 * 
		 * Implement mutual information. Each point will have two labels u and v corresponding to two different assignments, i.e., the
		 * 	same point was assigned to cluster (label) u in one assignment, and to cluster (label) v in the other assignment.
		 * 
		 * You must create one document for each cluster (label) in both u, whose _id must be u_label, and v, whose _id must be v_label.
		 * 	In addition to the _id, for each document, a field 'current_prob' must store the the probability of an element being assigned 
		 * 	to the cluster at hand (either in u or v). Furthermore, each document must contain a number of fields 'other_prob_i', each of
		 * 	them stores the probability of elements assigned to both the cluster at hand (either u_label or v_label), and cluster i in the
		 * 	other assignment, i.e., if the current document is storing u_j, 'other_prob_i' refers to v_i; similarly, if it storing v_j,
		 * 	'other_prob_i' refers to u_i.
		 * 
		 * We will always be using scale of 34 digits.
		 * 
		 */
		
		

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
