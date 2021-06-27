package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

//class FD1{
//	String lhs;
//	String rhs;
//	public String getLHS() {
//		return lhs;
//	}
//	public void setLHS(String lhs) {
//		this.lhs = lhs;
//	}
//	public String getRHS() {
//		return rhs;
//	}
//	public void setRHS(String rhs) {
//		this.rhs = rhs;
//	}
//}
class FD{
	Set<String> lhs;
	String rhs;
	public Set<String> getLHS() {
		return lhs;
	}
	public void setLHS(Set<String> lhs) {
		this.lhs = lhs;
	}
	public String getRHS() {
		return rhs;
	}
	public void setRHS(String rhs) {
		this.rhs = rhs;
	}
}
public class NaiveFDDiscovery{
	public static void main(String[] args) throws Exception {
		final String dbURL = args[0];
		final String user = args[1];
		final String pwd = args[2];
		final String relationName = args[3];
		final String outputFile = args[4];
//		
//		final String dbURL = "jdbc:mysql://localhost:3306/imdb_ibd";
//		final String user = "root";
//		final String pwd = "MySQLrox@123";
//		final String relationName = "person";
//		final String outputFile = "actor_relation.txt";
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		// These are the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// These are the functional dependencies discovered.
		Set<FD> fds = new HashSet<FD>();
//		Set<FD1> fds1 = new HashSet<FD1>();
		
		// TODO 0: Your code here!
		
		// Your program must be generic and work for any relation provided as input. You must read the names of the attributes from the input relation and store
		//	them in the attributes set.
		//
		// You must traverse the lattice of attributes starting from combinations of size 1. Remember that all the functional dependencies we will discover are
		//	of the form a1, ..., ak -> aj; therefore, there is a single attribute on the right-hand side, and one or more attributes on the left-hand side. Re-
		//	member also that we are not interested in trivial functional dependencies (right-hand side is included in left-hand side) or non-minimal (there exi-
		//	sts another functional dependency that is contained in the current one).
		//
		// To traverse the lattice, we start from single combinations of attributes in the left-hand side, then combinations of two attributes, then combinatio-
		//	ns of three attributes, etc. We stop when we have tested all possible combinations. Use Sets.combinations to generate these combinations.
		//
		// a1, a2 -> a3 is a functional dependency for relation x if the following SQL query outputs no result: 
		//	SELECT * FROM x AS t1 JOIN x AS t2 ON t1.a1 = t2.a1 AND t1.a2 = t2.a2 WHERE t1.a3 <> t2.a3
		//
		//	You must compose this type of SQL for the different combinations of attributes to find functional dependencies.
		
		// relationName: Parse relation name. Patient, Movie..
		// run a simple SQL query: select * from relationName
		PreparedStatement ps = con.prepareStatement("SELECT * FROM " +relationName+" LIMIT 1");
		ResultSet rs = ps.executeQuery();
		ResultSetMetaData meta = rs.getMetaData();
		int numberOfColumns = meta.getColumnCount();
		for(int size = 1; size <= numberOfColumns; size = size + 1)
			attributes.add(meta.getColumnName(size));
		PrintWriter writer = new PrintWriter(new File(outputFile));
		int flag = 0;
		for( int size = 1; size < attributes.size(); size++) {
			for (Set<String> leftHandSide : Sets.combinations(attributes, size)){
				for(String rightHandSide : attributes) {
					FD fd = new FD();
					fd.setLHS(leftHandSide);
					fd.setRHS(rightHandSide);
					if (leftHandSide.contains(rightHandSide))
						continue;
					for(FD f : fds) { 
						if (f.getRHS().equals(rightHandSide) && leftHandSide.containsAll(f.getLHS())) {
							flag = 1;
							break;
							}
					}
					if(flag == 1) {
						flag = 0;
						continue;
					}
						
					StringBuffer query = new StringBuffer("Select * from "+relationName+" as t1 join "+relationName+" as t2 on ");
					for(String aInLHS : leftHandSide)
						query.append("t1."+aInLHS+"=t2."+aInLHS+" and ");
					query.delete(query.length()-4, query.length());
					query.append("where t1."+ rightHandSide +" <> t2."+rightHandSide);
					query.append(" limit 1;");	// this checks if even 1 doesn't follow the fd rule instead of checking if all don't follow
					PreparedStatement psForCheckingFD = con.prepareStatement(query.toString());
					ResultSet counterExample = psForCheckingFD.executeQuery();
					if (counterExample.next()) {
					}
					else {
						fds.add(fd);
					}
					counterExample.close();
				}
				
			}
		}
		for(FD i: fds) {
			String[] temp = new String[i.getLHS().size()];
			int count = 0;
			for(String j: i.getLHS()) {
				temp[count] = j;
				count = count + 1;
			}
			Arrays.sort(temp);
			String temp1 =String.join(", ", temp);
			writer.println(temp1 + " -> " + i.getRHS());
		}
		writer.close();
		con.close();
	}
}
		
		