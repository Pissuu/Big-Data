package edu.rit.ibd.a6;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class KMeans {
	private static final String MANHATTAN = "Manhattan", EUCLIDEAN = "Euclidean", ARITHMETIC = "Arithmetic",
			GEOMETRIC = "Geometric";

	public static void main(String[] args) throws Exception {
		final String mongoDBURL = args[0];
		final String mongoDBName = args[1];
		final String mongoColPoints = args[2];
		final int k = Integer.valueOf(args[3]);
		final int maxEpochs = Integer.valueOf(args[4]);
		final String distance = args[5]; // Manhattan or Euclidean
		final String mean = args[6]; // Arithmetic or Geometric
		final long seed = Long.valueOf(args[7]);
		
//		final String mongoDBURL = "None";
//		final String mongoDBName = "root";
//		final String mongoColPoints = "boo2";
//		final int k = 50;
//		final int maxEpochs = 50;
//		final String distance = MANHATTAN; // Manhattan or Euclidean
//		final String mean = ARITHMETIC; // Arithmetic or Geometric
//		final long seed = Long.valueOf(5);

		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		Random rnd = new Random(seed);

		// TODO Your code here!

		/*
		 * 
		 * Implement K-Means. First, get the dimension of the points. You must assume
		 * that all points have the same dimension.
		 * 
		 * Initialize the centroids randomly (use the rnd provided). Remember that we
		 * are working with points that are normalized between 0 and 1. To ensure
		 * reproducibility and, therefore, the results will be equal as the grading
		 * software, centroids must be initialized in ascending order; within a
		 * centroid, dimensions should be initialized in ascending order.
		 * 
		 * Iterate until maxEpochs is reached or the previous centroids and the current
		 * centroids have not changed, i.e., the existing and new centroids are all
		 * equal.
		 * 
		 * In each iteration, assign each point to the closest cluster based on the
		 * input distance.
		 * 
		 * Compute the new centroid of cluster i as the mean (arithmetic or geometric,
		 * depending on input parameter) of the points assigned to i.
		 * 
		 * If there is a cluster i without any points assigned to it, re-initialize the
		 * centroid randomly (use the rnd provided). To ensure reproducibility and,
		 * therefore, the results will be equal as the grading software, centroids must
		 * be re-initialized in ascending order; within a centroid, dimensions should be
		 * re-initialized in ascending order.
		 * 
		 * We will always be using scale of 34 digits.
		 * 
		 */

		MongoCollection<Document> col = db.getCollection(mongoColPoints);
		int pointDimension = db.getCollection(mongoColPoints).find().first().getList("point", Decimal128.class).size();

		// Initialize centroids. centroid u1 should come before centroid u2.
		// For each centroid initialize dim_1, dim_2, dim_3...
		// rnd.nextDouble();
		// figure out the dimensions of the points.

		BigDecimal[][] clusterPoints = new BigDecimal[k][pointDimension];
		ArrayList<BigDecimal> clusterSse = new ArrayList<BigDecimal>();
		for(int i=0;i<k;i++) {
			for(int j=0;j<pointDimension;j++) {
				clusterPoints[i][j]=new BigDecimal(rnd.nextDouble());
			}
		}
		BigDecimal[][] oldClusterPoints = new BigDecimal[k][pointDimension];
		
		int iterations = 0;
		while (!shouldStop(oldClusterPoints, clusterPoints, maxEpochs, iterations,k)) {
			
			for (int i = 0; i < k; i++) {
				for (int j = 0; j < pointDimension; j++) {
					oldClusterPoints[i][j] = clusterPoints[i][j];
				}
			}																				
			
			clusterSse = new ArrayList<BigDecimal>();
			HashMap<Integer, ArrayList<List<Decimal128>>> minDistMap = new HashMap<Integer, ArrayList<List<Decimal128>>>();
			HashMap<Integer, ArrayList<BigDecimal>> minDistSse = new HashMap<Integer, ArrayList<BigDecimal>>();
			System.out.println("Iteration: "+iterations);
			for (int i = 0; i < k; i++) {
				minDistMap.put(i, new ArrayList<List<Decimal128>>());
				minDistSse.put(i, new ArrayList<BigDecimal>());
			}
			
			//Find distance for each point and save the values for sse and new centroid computation.
			for (Document doc : col.find().batchSize(5)) {
				List<Decimal128> dimensions = doc.getList("point", Decimal128.class);
				BigDecimal minDist = BigDecimal.valueOf(9999999);
				int label = -1;
				for (int cluster = 0; cluster < clusterPoints.length; cluster++) {

					BigDecimal d = BigDecimal.ZERO;
					if(distance.equals(MANHATTAN)) {
						d = manhattenDistance(clusterPoints[cluster], dimensions);
						//System.out.println("d: "+d);
					}
					else {
						d = euclidianDistance(clusterPoints[cluster], dimensions);
					}
					if (d.compareTo(minDist) == -1) {
						minDist = d;
						//System.out.println("mind: "+minDist);
						label = cluster;
					}
				}
				minDistSse.get(label).add(minDist);
				minDistMap.get(label).add(dimensions);
				col.updateOne(Filters.eq("_id", doc.get("_id", Integer.class)), Updates.set("label", label));
			}

			//new centroid computation
			for (int i = 0; i < k; i++) {
				if(!minDistMap.containsKey(i) || minDistMap.get(i).size() == 0) {
					//System.out.println("in equals: "+i);
					for (int j = 0; j < pointDimension; j++) {
						clusterPoints[i][j] = new BigDecimal(rnd.nextDouble());
					}
					clusterSse.add(BigDecimal.ZERO);
					continue;
				}
				ArrayList<List<Decimal128>> minDistPs = minDistMap.get(i);
				
				for (BigDecimal p : clusterPoints[i]) {
					p = BigDecimal.ZERO;
				}
				
				for (List<Decimal128> minP : minDistPs) {
					for (int j = 0; j < pointDimension; j++) {
						BigDecimal temp = minP.get(j).bigDecimalValue();
						clusterPoints[i][j] = clusterPoints[i][j].add(temp,MathContext.DECIMAL128);
					}
				}
				for (int j = 0; j < pointDimension; j++) {
					clusterPoints[i][j] = clusterPoints[i][j].divide(new BigDecimal(minDistPs.size()),MathContext.DECIMAL128);
				}
				
				BigDecimal sse = BigDecimal.ZERO;
				for (BigDecimal dist : minDistSse.get(i)) {
					sse = sse.add(dist.pow(2),MathContext.DECIMAL128);
				}
				clusterSse.add(sse);
			}


			iterations++;
		}

		// Store in MongoDb
		// Centroids: point, lable1, sse
		for (int i = 0; i < k; i++) {
			Document d = new Document();
			d.append("label", i);
			d.append("sse", clusterSse.get(i).round(MathContext.DECIMAL128));
			List<BigDecimal> pointsToInsert = new ArrayList<>();
			for (BigDecimal p : clusterPoints[i]) {
				pointsToInsert.add(p.round(MathContext.DECIMAL128));
			}
			d.append("point", pointsToInsert);
			System.out.println(d);
			col.insertOne(d);
		}

		// TODO End of your code!

		client.close();
	}

	private static Boolean shouldStop(BigDecimal[][] oldCentroids, BigDecimal[][] centroids, int maxEpochs,
			int iterations, int k) {
		if (iterations >= maxEpochs)
			return true;
		for (int i = 0; i < k; i++) {
			for (int j = 0; j < centroids[0].length; j++) {
				if(oldCentroids[i][j] != centroids[i][j]) {
					return false;
				}
			}
		}
		return true;
	}
	
	private static BigDecimal manhattenDistance(BigDecimal[] p1,List<Decimal128> p2) {
		BigDecimal dist = BigDecimal.ZERO;
		for (int i = 0; i < p1.length; i++) {
			BigDecimal temp = p1[i].subtract(p2.get(i).bigDecimalValue(),MathContext.DECIMAL128).abs(MathContext.DECIMAL128);
			dist = dist.add(temp,MathContext.DECIMAL128);
		}
		return dist;
	}
	
	private static BigDecimal euclidianDistance(BigDecimal[] p1, List<Decimal128> p2) {
		BigDecimal dist = BigDecimal.ZERO;
		for (int i = 0; i < p1.length; i++) {
			BigDecimal diffPow = p1[i].subtract(p2.get(i).bigDecimalValue(),MathContext.DECIMAL128).pow(2,MathContext.DECIMAL128);
			dist = dist.add(diffPow,MathContext.DECIMAL128);
		}
		dist = dist.sqrt(MathContext.DECIMAL128);
		return dist;
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