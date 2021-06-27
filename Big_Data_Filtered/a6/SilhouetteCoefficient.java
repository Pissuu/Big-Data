package edu.rit.ibd.a6;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class SilhouetteCoefficient {
	private static final String MANHATTAN = "Manhattan", EUCLIDEAN = "Euclidean";

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColClusters = args[2];
//		mongoColClusters has points, label, and points
		final String distance = args[3]; // Manhattan or Euclidean
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		MongoCollection<Document> col = db.getCollection(mongoColClusters);

		int no_of_dimensions = 0;
		for(Document j : col.find().batchSize(5)) {
			List<Decimal128> list1 = new ArrayList<Decimal128>();
			list1 = j.getList("point", Decimal128.class);
			no_of_dimensions = list1.size();
			break;
		}
		
		Set<Integer> unique_clusters = new HashSet<Integer>();
		for(Document point: col.find().batchSize(5)) {
			int label = point.getInteger("label");
			unique_clusters.add(label);
		}
		HashMap<Integer,Integer> cluster_hm = new HashMap<Integer, Integer>();
		Iterator<Integer> uc = unique_clusters.iterator();
		int counter = 0;
	     while(uc.hasNext()){
	    	 cluster_hm.put(counter, uc.next());
	    	 counter = counter + 1;
	      }
//	     System.out.println(cluster_hm);
		
		List<List<Document>> list_of_clusters = new ArrayList<List<Document>>();
		for(int i = 0; i < unique_clusters.size(); i++) {
			List<Document> temp = new ArrayList<Document>();
			for(Document j: col.find().batchSize(5)) {
				int pointLabel = j.getInteger("label");
				if(pointLabel == cluster_hm.get(i))
					temp.add(j);
			}
			list_of_clusters.add(temp);
		}
		for(int i = 0; i < unique_clusters.size(); i++) {
//			System.out.println("we are creating a and we are in cluster number" + i);
			for (int k = 0; k < list_of_clusters.get(i).size(); k++) {
				Document point1 = list_of_clusters.get(i).get(k);
				BigDecimal sum = new BigDecimal(0);
				for(int j = 0; j < list_of_clusters.get(i).size(); j++) {
					Document point2 = list_of_clusters.get(i).get(j);
					if(distance.equals("Manhattan"))
						sum = sum.add(Manhattan(point1.getList("point", Decimal128.class), point2.getList("point", Decimal128.class)), MathContext.DECIMAL128);
					else
						sum = sum.add(Euclidean(point1.getList("point", Decimal128.class), point2.getList("point", Decimal128.class)), MathContext.DECIMAL128);
				}
				sum = sum.divide(new BigDecimal(list_of_clusters.get(i).size(), MathContext.DECIMAL128), MathContext.DECIMAL128);
				point1.append("a", sum);
//				if(point1.getInteger("_id") == 290095 || point1.getInteger("_id") == 450315) 
//					System.out.println(point1 + "being divided by " + list_of_clusters.get(i).size());
				Document new1 = new Document().append("$set", new Document().append("a", sum));
				col.updateOne(Document.parse("{_id:" + point1.getInteger("_id") + "}"), new1);
//				col.updateOne(Document.parse("{_id:" + point1.getInteger("_id") + "}"), Document.parse("{$set : {a:" + sum + "}}"));

			}
		}
//		System.out.println("hereererer");
//		System.out.println(col.find(new Document().append("_id",450315)).first());
//		System.out.println(col.find(new Document().append("_id",290095)).first());
		
		for(int i = 0; i < list_of_clusters.size(); i++) {
			for(int j = 0; j <list_of_clusters.get(i).size(); j++) {
				Document d1 = list_of_clusters.get(i).get(j);
				for(int k = 0; k < list_of_clusters.size(); k++) {
					if( k==i)
						continue;
					else {
						BigDecimal sum = new BigDecimal(0);
						for(int x= 0; x < list_of_clusters.get(k).size(); x++) {
							Document d2 = list_of_clusters.get(k).get(x);
							if(distance.equals("Manhattan"))
								sum = sum.add(Manhattan(d1.getList("point", Decimal128.class), d2.getList("point", Decimal128.class)), MathContext.DECIMAL128);
							else
								sum = sum.add(Euclidean(d1.getList("point", Decimal128.class), d2.getList("point", Decimal128.class)), MathContext.DECIMAL128);
						}
						sum = sum.divide(new BigDecimal(list_of_clusters.get(k).size(), MathContext.DECIMAL128), MathContext.DECIMAL128);
						String cluster_distance = "d_" + String.valueOf(cluster_hm.get(k));
//						col.updateOne(Document.parse("{_id:" + d1.getInteger("_id") + "}"), Document.parse("{$set : {" + cluster_distance + ":"+ sum.abs(MathContext.DECIMAL128) + "}}"));
						Document new2 = new Document().append("$set", new Document().append(cluster_distance, sum));
						col.updateOne(Document.parse("{_id:" + d1.getInteger("_id") + "}"), new2);
//						d1.append(cluster_distance, sum.abs(MathContext.DECIMAL128));
					}
				}
//				if(d1.getInteger("_id") == 450315 || d1.getInteger("_id") == 290095) {
//					System.out.println(d1);
//				}
			}
//			System.out.println("we are at cluster number " + i);
		}
//		System.out.println("why not check here");
//		System.out.println(col.find(new Document().append("_id",450315)).first());
//		System.out.println(col.find(new Document().append("_id",290095)).first());
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
	// we will calculate Manhattan Distance
	private static BigDecimal Manhattan(List<Decimal128> p1,List<Decimal128> p2) {
		BigDecimal sum = new BigDecimal(0);
		for(int i = 0; i < p1.size(); i++) {
			BigDecimal difference1 = p1.get(i).bigDecimalValue().subtract(p2.get(i).bigDecimalValue(), MathContext.DECIMAL128);
			sum = sum.add(difference1.abs(),MathContext.DECIMAL128);
		}
		return sum.abs(MathContext.DECIMAL128);
	}
	//we will calculate Euclidean distance
	private static BigDecimal Euclidean(List<Decimal128> p1,List<Decimal128> p2) {
		BigDecimal sum = new BigDecimal(0);
		for(int i = 0; i < p1.size(); i++) {
			BigDecimal difference = new BigDecimal(0);
			difference = p1.get(i).bigDecimalValue().subtract(p2.get(i).bigDecimalValue(), MathContext.DECIMAL128);
			difference = difference.multiply(difference,MathContext.DECIMAL128);
			sum = sum.add(difference, MathContext.DECIMAL128);
		}
		sum = sum.sqrt(MathContext.DECIMAL128);
		
		return sum.abs(MathContext.DECIMAL128);
	}

}
