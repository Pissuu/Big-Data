package edu.rit.ibd.a1;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class IMDBFilesToMySQL{
	public static void main(String[] args) throws Exception {
		final String jdbcURL = args[0];
		final String jdbcUser = args[1];
		final String jdbcPwd = args[2];
		final String folderToIMDBGZipFiles = args[3];
		Connection con = DriverManager.getConnection(jdbcURL, jdbcUser, jdbcPwd);
		con.setAutoCommit(false); //all operations are automatically committed
		 //SIMPLE TABLES CREATION 
		String[] drop_tables = new String[]{"actor","director","producer","writer","moviegenre","person","movie","genre"};
		for(int i = 0; i< drop_tables.length; i++) {
			PreparedStatement st = con.prepareStatement("drop table if exists "+ drop_tables[i]);
			st.execute();st.close();con.commit();	
		}
		String[] create_tables = new String[]{"actor","director","producer","writer",};
		for(int i = 0; i< create_tables.length; i++) {
			PreparedStatement st = con.prepareStatement("create table "+ create_tables[i] + "(pid integer, mid integer, primary key(pid, mid))");
			st.execute();st.close();con.commit();	
		}
		
		PreparedStatement st = con.prepareStatement("create table moviegenre(gid integer, mid integer, primary key(gid, mid))");
		st.execute();st.close();con.commit();	
		st = con.prepareStatement("create table person(id integer,name varchar(150), birthyear integer, deathyear integer, primary key(id))");
		st.execute();st.close();con.commit();
		st = con.prepareStatement("create table genre(id integer, name varchar(150), primary key(id))");
		st.execute();st.close();con.commit();
		st = con.prepareStatement("create table movie(id integer, title varchar(250), isadult boolean, runtime integer,year integer, rating float, votes integer, primary key (id))");
		st.execute();st.close();con.commit();
		
		//---------------------------------------------------MOVIES
		//SIMPLE, DONE IN RECITATION CLASS
		st = con.prepareStatement("insert into movie(id, title, isAdult,year, runtime, rating, votes) values (?,?,?,?,?,?,?)");
		int countLines = 0;
		InputStream gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		InputStream gzipStream1 = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.ratings.tsv.gz"));
		Scanner sc = new Scanner(gzipStream, "UTF-8");
		Scanner sc1 = new Scanner(gzipStream1, "UTF-8");
		String currentLine1 = sc1.nextLine();
		currentLine1 = sc1.nextLine();
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();
			countLines++;
			String[] splitLine = currentLine.split("\t");
			if(splitLine[1].equals("movie") || splitLine[1].equals("tvMovie")) {
				//movie id
				int id = Integer.valueOf(splitLine[0].replace("tt",""));
				st.setInt(1, id);
				//movie title
				st.setString(2, splitLine[3]);
				//is adult
				st.setBoolean(3, splitLine[4].equals("1"));
				//movie year
					try {
					st.setInt(4, Integer.valueOf(splitLine[5]));
					} catch(Throwable oops) {
						st.setNull(4, Types.INTEGER);
					}
				
				//runtime
				if(!splitLine[7].equals("\\N"))
					st.setInt(5,  Integer.valueOf(splitLine[7]));
				else
					st.setNull(5, Types.INTEGER);
				//ratings
				
				String[] splitLine1 = currentLine1.split("\t");
				int id1 = Integer.parseInt(splitLine1[0].replace("tt", ""));
				while(id1 < id & sc1.hasNext()) {
					currentLine1 = sc1.nextLine();
					splitLine1 = currentLine1.split("\t");
					id1 = Integer.parseInt(splitLine1[0].replace("tt", ""));
				}
				if(id1 == id) {
					st.setFloat(6, Float.parseFloat(splitLine1[1]));
					//votes
					st.setInt(7, Integer.parseInt(splitLine1[2]));
				}
				else {
					st.setNull(6, Types.FLOAT);
					//votes
					st.setNull(7, Types.INTEGER);
				}
				st.addBatch(); //adds currentline to batch
				
			}
						
			if (countLines % 1000 == 0) {
				System.out.println(new Date()+ "-- Movies Processed so far: " + countLines);
				st.executeBatch(); //execute the batch
				con.commit(); //reflect into data base
			}
		}
		st.executeBatch(); //execute the batch
		con.commit(); //reflect into data base
		st.close();
		
		
		//-------------------------------------GENRE
		st = con.prepareStatement("insert into genre(id, name) values (?,?)");
		countLines = 0;
		HashSet<String> genres = new HashSet<String>();
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		while (sc.hasNextLine()) {
			String currentLine = sc.nextLine();			
			countLines++;
			if(countLines <= 10)
				System.out.println(currentLine);
			String[] splitLine = currentLine.split("\t");
			//CHECK IF THE TITLE TYPE IS MOVIE OR TV SHOW
			if(!splitLine[8].equals("\\N") & (splitLine[1].equals("movie") || splitLine[1].equals("tvMovie"))) {
				String temp="";
				temp = splitLine[8];
				String current_genres[];
				if(temp.contains(",")) {
					current_genres = temp.split(",");
					for(int i = 0; i < current_genres.length; i++) 
						//PUSH VALUES INTO A HASHSET SO THAT DUPLICATES ARE NON EXISTANT
						genres.add(current_genres[i]);
				}
				//IF THE COLUMN IS NOT CSV
				else
					genres.add(temp);	
			}
		}
		Iterator<String> it = genres.iterator();
		int gid = 0;
	    while(it.hasNext()){
	    	 st.setInt(1, gid );
	    	 st.setString(2, it.next());
	    	 st.addBatch();
	    	 gid = gid + 1;
	     }
		st.executeBatch();
		con.commit(); //reflect into data base
		
		
		//----------------------------------------------PERSON
		//SIMPLE AND STRAIGHT FORWARD, NO COMPLICATED CONDITIONS
		st = con.prepareStatement("insert into person(id, name, birthyear, deathyear) values (?,?,?,?)");
		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"name.basics.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		String currentLine = sc.nextLine();			
		while (sc.hasNextLine()) {
			currentLine = sc.nextLine();			
			countLines++;
			if(countLines <= 10)
				System.out.println(currentLine);
			String[] splitLine = currentLine.split("\t");
			st.setInt(1, Integer.parseInt(splitLine[0].replace("nm","")));
			st.setString(2, splitLine[1]);
			try {
				st.setInt(3, Integer.parseInt(splitLine[2]));
			} catch(Exception oops) {
				st.setNull(3, Types.INTEGER);
			}
			try {
				st.setInt(4, Integer.parseInt(splitLine[3]));
			}	catch(Exception oops) {
				st.setNull(4, Types.INTEGER);
			}
			st.addBatch();
			if(countLines % 1000 == 0) {
				System.out.println(new Date()+ "-- Person Processed so far: " + countLines);
				st.executeBatch();
				con.commit(); //reflect into data base
				}
		}
		sc.close();
		st.executeBatch();
		con.commit();
		
		//-----------------------MOVIE GENRE
		//VERY SIMPLE
		st = con.prepareStatement("insert into moviegenre(gid, mid ) values (?,?)");
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		currentLine = sc.nextLine();	
		String query = "select id,name from genre where id > -1";
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(query);
		Hashtable<String,Integer> gid_hash = new Hashtable<String,Integer>();
		//INSERT ALL GENRES AND THEIR ID INTO HASHTABLE TO ALLOW O(1) TIME COMPLEXITY FOR ACCESS OF DATA
		while(rs.next()) {
//			2 has the name
//			1 has the id
			gid_hash.put(rs.getString(2),Integer.parseInt(rs.getString(1)));
		}
//			System.out.println(gid_hash);
//		System.exit(0);
		countLines = 0;
		while (sc.hasNextLine()) {
			countLines = countLines + 1;
			currentLine = sc.nextLine();			
			String[] splitLine = currentLine.split("\t");
			//ONLY INTERESTED IN MOVIE AND TVMOVIE DATA
			if(splitLine[1].equals("movie") || splitLine[1].equals("tvMovie")) {
				int movie_id = Integer.parseInt(splitLine[0].replace("tt",""));
				String[] genres_movie = splitLine[8].split(",");
				//EACH MOVIE CAN HAVE MULITPLE GENRES IN CSV FORM
				if(genres_movie[0].equals("\\N"));
				else
					//MATCH EACH GENRE WITH ITS ID FROM HASHTABLE
				for(int i = 0; i < genres_movie.length; i++) {
						gid = gid_hash.get(genres_movie[i]);
						st.setInt(1, gid);
						st.setInt(2, movie_id);
						st.addBatch();
				}
				if(countLines % 1000 == 0) {
					System.out.println(new Date()+ "-- Movie Genre Processed so far: " + countLines);
					st.executeBatch();
					con.commit(); //reflect into data base
				}
			}
		}
		sc.close();
		st.executeBatch();
		con.commit();
		
		// ---------------------------------------WRITER
		
		st = con.prepareStatement("insert into writer(pid, mid ) values (?,?)");
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.crew.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		currentLine = sc.nextLine();	
		String[] splitLine;
		countLines = 0;
		stmt = con.createStatement();
		//INSERT ALL MOVIE IDS INTO HASH SET TO ENABLE CHECKING OF MOVIE ID IN MOVIE TABLE 
		rs = stmt.executeQuery("select id from movie where id > -1");
		Set<Integer> mid_set = new HashSet<Integer>();
		Map<Integer, HashSet> writer_map = new HashMap<Integer, HashSet>();
		HashSet<Integer> hs;
		//CONVERT ALL MOVIE IDS INTO HASH SET
		while(rs.next())
			mid_set.add(rs.getInt("id"));
		int count  = 0;
		while(sc.hasNext()) {
			count = count + 1;
			if(count % 100000 == 0)
				System.out.println("Processed from title.crew, number of lines is " + count);
			splitLine = sc.nextLine().split("\t");
			int mid = Integer.parseInt(splitLine[0].replace("tt",""));
			if(!splitLine[2].equals("\\N") & mid_set.contains(mid)){ //checks that null is not there in writer column and that the tconst is a movie/TVshow id
				String[] writers = splitLine[2].split(",");
				hs = new HashSet();
				for(String i : writers)
					hs.add(Integer.parseInt(i.replace("nm","")));
				//UPDATE HASH MAP WITH THE MOVIE ID AND THE WRITER ID
				writer_map.put(mid, hs);
			}
		}
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.principals.tsv.gz"));	
		sc = new Scanner(gzipStream, "UTF-8");
		count = 0;
		sc.nextLine();
		while(sc.hasNext()) {
			count = count + 1;
			if(count % 100000 == 0)
				System.out.println("Processed from principals.crew, number of lines is " + count);
			splitLine = sc.nextLine().split("\t");
			int mid = Integer.parseInt(splitLine[0].replace("tt",""));
			if(!splitLine[3].equals("\\N") & mid_set.contains(mid) & splitLine[3].contains("writer")){
				int writer = Integer.parseInt(splitLine[2].replace("nm", ""));
				if(writer_map.containsKey(mid))
					writer_map.get(mid).add(writer);
				else {
					hs = new HashSet();
					hs.add(writer);
					writer_map.put(mid, hs);
				}
			}
		}
		int temp;
		for(int i : writer_map.keySet()) {
			Iterator<Integer> j = writer_map.get(i).iterator();
			while(j.hasNext()) {
				temp = j.next();
				rs = stmt.executeQuery("select id from person where id = '" + temp + "'");
				if(rs.next()) {
					st.setInt(2, i);
					st.setInt(1, temp);
					st.addBatch();
					countLines = countLines + 1;
				}
				if(countLines % 1000 == 0) {
					System.out.println(new Date()+ "-- Writer Processed so far: " + countLines);
					st.executeBatch();
					con.commit(); //reflect into data base
				}
			}
		}
		st.executeBatch();
		con.commit();
		
		//------------------------------------------------------------PRODUCER
		//VERY STRAIGHT FORWARD AND SIMPLE
		st = con.prepareStatement("insert into producer(pid, mid ) values (?,?)");
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.principals.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		sc.nextLine();
		currentLine = sc.nextLine();	
		countLines = 0;
		while(sc.hasNext()) {
			splitLine = sc.nextLine().split("\t");
			int pid = Integer.parseInt(splitLine[2].replace("nm",""));
			String[] mids = splitLine[0].split(",");
			if(splitLine[3].contains("producer") & !mids[0].equals("\\N")){
				for(int i = 0; i < mids.length ; i++) {
					if(mid_set.contains(Integer.parseInt(mids[i].replace("tt", "")))) {
						countLines = countLines + 1;
						st.setInt(1, pid);
						st.setInt(2, Integer.parseInt(mids[i].replace("tt", "")));
						st.addBatch();
					}
				}
				if(countLines % 1000 == 0) {
					System.out.println(new Date()+ "--Producer Processed so far: " + countLines);
					st.executeBatch();
					con.commit(); //reflect into data base
				}
			}
		}
		st.executeBatch();
		con.commit();
		
		
	//-------------------------------------------DIRECTOR
		//SIMILAR TO WRITER
		st = con.prepareStatement("insert into director(pid, mid ) values (?,?)");
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.crew.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		sc.nextLine();
		currentLine = sc.nextLine();	
		countLines = 0;
		HashSet<Integer> director_set;
		writer_map = new HashMap<Integer, HashSet>();
		int counter = 0;
		count  = 0;
		int current_dir;
		while(sc.hasNext()) {
			count = count + 1;
			if(count % 100000 == 0)
				System.out.println("Processed from title.crew, number of lines is " + count);
			splitLine = sc.nextLine().split("\t");
			int mid = Integer.parseInt(splitLine[0].replace("tt",""));
			if(!splitLine[1].equals("\\N") & mid_set.contains(mid)){
				String[] directors = splitLine[1].split(",");
				director_set = new HashSet();
				for(int i = 0; i < directors.length ; i++) {
//					director_set = new HashSet<Integer>();
					counter = counter + 1;
					current_dir = Integer.parseInt(directors[i].replace("nm", ""));
					director_set.add(Integer.parseInt(directors[i].replace("nm", "")));
				}
					writer_map.put(mid, director_set);
			}
		}
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.principals.tsv.gz"));	
		sc = new Scanner(gzipStream, "UTF-8");
		count = 0;
		splitLine = sc.nextLine().split("\t");
		while(sc.hasNext()) {
			count = count + 1;
			if(count % 100000 == 0)
				System.out.println("Processed from principals.crew, number of lines is " + count);
			splitLine = sc.nextLine().split("\t");
			int mid = Integer.parseInt(splitLine[0].replace("tt",""));
			if(!splitLine[3].equals("\\N") & mid_set.contains(mid) & splitLine[3].contains("director")){
				countLines = countLines + 1;
				int director = Integer.parseInt(splitLine[2].replace("nm" , ""));
				if(writer_map.containsKey(mid)) {
					writer_map.get(mid).add(director);
				}
				else {
					director_set = new HashSet();
					director_set.add(director);
					writer_map.put(mid, director_set);
				}
			}
		}
		System.out.println(counter);
		for(int i : writer_map.keySet()) {
			Iterator<Integer> j = writer_map.get(i).iterator();
			while(j.hasNext()) {
				temp = j.next();
				rs = stmt.executeQuery("select id from person where id = '" + temp + "'" );
				if(rs.next()) {
					st.setInt(2, i);
					st.setInt(1, temp);
					st.addBatch();
					countLines = countLines + 1;
					if(countLines % 1000 == 0) {
						System.out.println(new Date()+ "--Director  Processed so far: " + countLines);
						st.executeBatch();
						con.commit(); //reflect into data base
					}
				}
			}
		}
		st.executeBatch();
		con.commit();
		
		//--------------------ACTOR
		//CONDITIONS REQUIRE TO CHECK IF ACTOR EXISTS IN PERSON OR NOT
		st = con.prepareStatement("insert into actor(pid, mid ) values (?,?)");
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.principals.tsv.gz"));		
		sc = new Scanner(gzipStream, "UTF-8");
		sc.nextLine();
		currentLine = sc.nextLine();	
		countLines = 0;
		stmt = con.createStatement();
		Map<Integer, HashSet > act_set = new HashMap<Integer, HashSet>();
		int limit = 0;
		HashSet actors;
		while(sc.hasNext()) {
			limit = limit + 1;
			if(limit % 1000 == 0)
				System.out.println("finished non sql processing for lines ----" + limit);
			splitLine = sc.nextLine().split("\t");
			int movie_id = Integer.parseInt(splitLine[0].replace("tt", ""));
//			System.out.println(splitLine[3]);
			if(mid_set.contains(movie_id) & (splitLine[3].contains("act")|| splitLine[3].equals("self"))) {
				if(act_set.containsKey(movie_id)) 
					act_set.get(movie_id).add(Integer.parseInt(splitLine[2].replace("nm","")));
				else {
					actors = new HashSet<Integer>();
					actors.add(Integer.parseInt(splitLine[2].replace("nm", "")));
					act_set.put(movie_id, actors);
				}
			}
		}
		for (Integer i : act_set.keySet()) {
			Iterator<Integer> j = act_set.get(i).iterator();
			while(j.hasNext()) {
				temp = j.next();
				//CHECK IF ACTOR ID IS IN PERSON ID OR NOT
				rs = stmt.executeQuery("select id from person where id = '" + temp + "'" );
				if(rs.next()) {
					countLines = countLines + 1;
					st.setInt(2, i);
					st.setInt(1, temp);
					st.addBatch();
					if(countLines % 1000 == 0) {
						System.out.println(new Date()+ "-- Actor Processed so far: " + countLines);
						st.executeBatch();
						con.commit(); //reflect into data base
					}
				}
			}
		}
		sc.close();
		st.executeBatch();
		con.commit();
		st.close();
		con.close();
	}
}