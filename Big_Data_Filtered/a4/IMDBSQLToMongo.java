package edu.rit.ibd.a4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String mongoDBURL = args[3];
		final String mongoDBName = args[4];
		System.out.println(new Date() + " -- Started");
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		MongoCollection<Document> moviesdenorm = db.getCollection("MoviesDenorm");
		MongoCollection<Document> movies = db.getCollection("Movies");
		moviesdenorm.drop();
		movies.drop();
		PreparedStatement st = con.prepareStatement("SELECT id, title, isAdult, year, runtime, rating, votes from Movie"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		ResultSet rs = st.executeQuery();
		while (rs.next()) {
			Document d = new Document();
			StringBuffer buf = new StringBuffer();
			if(rs.getString("id")!=null)
				d.append("_id", rs.getInt("id"));
			if(rs.getString("title")!=null)
				d.append("title", rs.getString("title"));
			if(rs.getString("isAdult")!=null)
				d.append("isAdult", rs.getInt("isAdult"));
			if(rs.getString("year")!=null)
				d.append("year", rs.getInt("year"));
			if(rs.getString("runtime")!=null)
				d.append("runtime", rs.getInt("runtime"));
			if(rs.getString("rating")!=null)
				d.append("rating", rs.getFloat("rating"));
			if(rs.getString("votes")!=null)
				d.append("votes", rs.getInt("votes"));
			movies.insertOne(d);
			moviesdenorm.insertOne(d);
		}
		System.out.println("we have finished movie basic and moviesdenorm basic");
		st = con.prepareStatement("SELECT MovieGenre.mid, genre.name  from MovieGenre JOIN genre on genre.id = MovieGenre.gid ORDER BY MovieGenre.mid"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			movies.updateOne(Document.parse("{_id:"+rs.getInt("mid")+"}"), 
					Document.parse("{$addToSet : { genres:'" + rs.getString("name") + "'}}"));
			moviesdenorm.updateOne(Document.parse("{_id:"+rs.getInt("mid")+"}"), 
					Document.parse("{$addToSet : { genres:'" + rs.getString("name") + "'}}"));
		}
		System.out.println("movies denorm genres");
		st = con.prepareStatement("SELECT pid, mid from actor"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			moviesdenorm.updateOne(Document.parse("{_id:"+rs.getInt("mid")+"}"), 
					Document.parse("{$addToSet : { actors:" + rs.getInt("pid") + "}}"));
		}
		System.out.println("movies denorm actor");
		st = con.prepareStatement("SELECT pid, mid from director"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			moviesdenorm.updateOne(Document.parse("{_id:"+rs.getInt("mid")+"}"), 
					Document.parse("{$addToSet : { directors:" + rs.getInt("pid") + "}}"));
		}
		System.out.println("movies denorm directors");
		st = con.prepareStatement("SELECT pid, mid from producer"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			moviesdenorm.updateOne(Document.parse("{_id:"+rs.getInt("mid")+"}"), 
					Document.parse("{$addToSet : { producers:" + rs.getInt("pid") + "}}"));
		}
		System.out.println("movies denorm producers");
		st = con.prepareStatement("SELECT pid, mid from writer"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			moviesdenorm.updateOne(Document.parse("{_id:"+rs.getInt("mid")+"}"), 
					Document.parse("{$addToSet : { writers:" + rs.getInt("pid") + "}}"));
		}	
		MongoCollection<Document> peopledenorm = db.getCollection("PeopleDenorm");
		System.out.println("movies denorm writers");
		
		MongoCollection<Document> people = db.getCollection("People");
		people.drop();
		peopledenorm.drop();
		st = con.prepareStatement("SELECT id, name, birthYear, deathYear from person"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			Document d = new Document();
			StringBuffer buf = new StringBuffer();
			if(rs.getString("id")!=null)
				d.append("_id", rs.getInt("id"));
			if(rs.getString("name")!=null)
				d.append("name", rs.getString("name"));
			if(rs.getString("birthYear")!=null)
				d.append("birthYear", rs.getInt("birthYear"));
			if(rs.getString("deathYear")!=null)
				d.append("deathYear", rs.getInt("deathYear"));
			people.insertOne(d);
			peopledenorm.insertOne(d);
		}
		System.out.println("people and people denorm basic");
		st = con.prepareStatement("SELECT pid, mid from actor"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			peopledenorm.updateOne(Document.parse("{_id:"+rs.getInt("pid")+"}"), 
					Document.parse("{$addToSet : {moviesActed:" + rs.getInt("mid") + "}}"));
		}	
		System.out.println("people denorm actor");
		st = con.prepareStatement("SELECT pid, mid from director"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			peopledenorm.updateOne(Document.parse("{_id:"+rs.getInt("pid")+"}"), 
					Document.parse("{$addToSet : {moviesDirected:" + rs.getInt("mid") + "}}"));
		}	
		System.out.println("people denorm director");
		st = con.prepareStatement("SELECT pid, mid from producer"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			peopledenorm.updateOne(Document.parse("{_id:"+rs.getInt("pid")+"}"), 
					Document.parse("{$addToSet : {moviesProduced:" + rs.getInt("mid") + "}}"));
		}	
		System.out.println("people denorm producer");
		st = con.prepareStatement("SELECT pid, mid from writer"); // Select from mapping table.
		st.setFetchSize(25); //how many tuples are retrieved every time
		rs = st.executeQuery();
		while (rs.next()) {
			peopledenorm.updateOne(Document.parse("{_id:"+rs.getInt("pid")+"}"), 
					Document.parse("{$addToSet : {moviesWritten:" + rs.getInt("mid") + "}}"));
		}	
		System.out.println("people denorm writer");
		rs.close();
		st.close();

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
