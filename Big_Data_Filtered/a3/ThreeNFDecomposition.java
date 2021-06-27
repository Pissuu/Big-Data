package edu.rit.ibd.a3;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Sets;
class FD{
	Set<String> lhs;
	Set<String> rhs;
	public Set<String> getLHS() {
		return lhs;
	}
	public void setLHS(Set<String> lhs) {
		this.lhs = lhs;
	}
	public Set<String> getRHS() {
		return rhs;
	}
	public void setRHS(Set<String> rhs) {
		this.rhs = rhs;
	}
	public String toString() {
		return lhs+" -> " +rhs;
		
	}
}
public class ThreeNFDecomposition {
	
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String cksStr = args[2];
		final String outputFile = args[3];
//		final String relation = "r(A,B,C)";
//		final String fdsStr = "A -> B;B -> C";
//		final String cksStr = "A";
//		final String outputFile = "output.txt";
		Set<String>  candidate_keys = new HashSet<String>();
		String[] ck = new String[CharMatcher.is(';').countIn(fdsStr)+1];	
		ck = cksStr.split(";");
		List<Set<String>> cks = new ArrayList<>();
		int counter = 0;
		while(counter < ck.length) {
			ck[counter] = ck[counter].trim();
//			System.out.println("ldfhajdfhljsafd" + ck[counter]);
			counter = counter + 1;
//			cks.addAll(ck[counter].split(", "));
		}
		counter = 0;
		while(counter < ck.length) {
			Set<String> n = new HashSet<>();
			String[] s = new String[CharMatcher.is(',').countIn(ck[counter])+1];
//			System.out.println("the counter " + ck[counter]);
			s = ck[counter].split(",");
//			System.out.println("we are here " + s.toString());
			for(String d: s) {
//				System.out.println(d);
				n.add(d.trim());
			}
//			System.out.println("we are here" + n.toString());
			cks.add(n);
			counter = counter +1; 
		}
//		System.out.println(cks);
//		cks.addAll()
		String rel = relation;
		rel = rel.substring(rel.indexOf("(")+1);
		rel = rel.substring(0,rel.indexOf(")"));
		String alph[] = rel.split(",");
		Set<String> attributes = new HashSet<String>();
		for(String i : alph)
			attributes.add(i.trim());
		// This stores the functional dependencies provided as input.
		Set<FD> fds = new HashSet<>();
		// This stores the candidate keys provided as input.
		// This stores the final 3NF decomposition, i.e., the output.
		List<Set<String>> decomposition = new ArrayList<>();
		String[] ifds = new String[CharMatcher.is(';').countIn(fdsStr)+1];
		ifds = fdsStr.split(";");
		String[] lr = new String[2];
		for(String i : ifds) {
			FD fd = new FD();
			lr[0] = i.split("->")[0];
			lr[1] = i.split("->")[1];
			Set<String> lhs = new HashSet<>();
			for(String j : lr[0].split(","))
				lhs.add(j.trim());
			Set<String> rhs = new HashSet<>();
			for(String j : lr[1].split(","))
				rhs.add(j.trim());
			fd.setLHS(lhs);
			fd.setRHS(rhs);
			fds.add(fd);
		}
//		for(FD f :fds)
//			System.out.println(f.toString());


		int count = 0;
		for(FD f:fds) {
			int flag = 0;
//			//checks trivial
			if(f.getRHS().containsAll(f.getLHS())) {
//				System.out.println("case1 fails");
				count = count + 1;
				continue;
			}
			if(closure(f.getLHS(), fds).containsAll(attributes)) {
//				System.out.println("case 2 fails for " + f.toString());
				count = count + 1;
				continue;
			}
			Set<String> temp = new HashSet<String>();
			temp.addAll(f.getRHS());
			temp.remove(f.getLHS());
//			System.out.println(temp);
			for(Set<String> j: cks) {
//				System.out.println("this is it " + j.toString());
				if(j.containsAll(temp)) {
//					System.out.println("Adfasdf");
					flag = 1;
					break;
				}
//				else
//					System.out.println("adf" + j.toString());
			}
			if(flag==1) {
				flag = 0;
				count = count + 1;
				continue;
			}
			decomposition(fds, cks, outputFile);
			}
//		System.out.println("its in 3nf");
		String output = "r(";
		for(String a:attributes) {
			output = output + a + ", ";
		}
		output = output.substring(0,output.length()-2);
		output = output + ")";
//		System.out.println(output);


		// TODO 0: Your code here!
		//
		// Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
		//
		// Parse the input functional dependencies. These are already a canonical cover. Recall that attributes can be formed by multiple letters.
		//
		// Parse the input candidate keys. Recall that attributes can be formed by multiple letters.
		//
		// Analyze whether the relation is already in 3NF:
		//	alreadyIn3NF=true
		//	For each FD A->B (A and B are sets of attributes):
		//		check = (B is included or equal in A) OR (A is superkey) OR (B \ A is contained in at least one candidate key)
		//		If !check: alreadyIn3NF=false, proceed to decompose!
		//	If alreadyIn3NF: Stop! We are done!
		//
		// Compute canonical cover of FDs (you must assume that the input is already a canonical cover).
		//

		
		
		
		// TODO 0: End of your code.
		PrintWriter writer = new PrintWriter(new File(outputFile));
		writer.println(output);
//		for (Set<String> r : decomposition)
//		writer.println("r(" + r.stream().sorted().collect(java.util.stream.Collectors.toList()).
//				toString().replace("[", "").replace("]", "") + ")");
//		for (Set<String> r : decomposition)
//			writer.println("r(" + r.stream().sorted().collect(java.util.stream.Collectors.toList()).
//					toString().replace("[", "").replace("]", "") + ")");
		writer.close();
}
	private static void decomposition(Set<FD> fds, List<Set<String>> ck, String op) throws Exception{
//		System.out.println("not in 3nf"); 
		PrintWriter writer = new PrintWriter(new File(op));

		List<String> l1 = new ArrayList<String>();
		int flag = 0;
		for(FD i : fds) {
			String start = "r(";
			for(String k: i.getRHS())
				l1.add(k.trim());
			for(String m: i.getLHS())
				l1.add(m.trim());
			Collections.sort(l1);
			Set<String> s1 = new HashSet<>();
			for(String c:l1)
				s1.add(c);
//			System.out.println("we are here");
//			System.out.println(s1.toString());
			for(Set<String> d: ck) {
				if(s1.containsAll(d)) {
					flag = 1;
//					System.out.println(s1.toString() + " contains " + d.toString());
				}
			}
//			s1.addAll(l1);
//			System.out.println();
			for(String idk : l1 )
				start = start + idk + ", ";
			start = start.substring(0, start.length()-2);
			String output = start + ")";
//			System.out.println(output);
			writer.println(output);

			l1.clear();
		}
		if(flag == 0) {
			String daba = "r(";
//			System.out.println("flag");
			List<String> lk = new ArrayList<>();
			for(Set<String> g: ck) {
				for(String d: g) {
					lk.add(d);
				}
				break;
			}
			Collections.sort(lk);
			for(String a: lk)
				daba = daba + a + ", ";
			daba= daba.substring(0, daba.length()-2);
			daba = daba + ")";
//			System.out.println("cak" + daba);
			writer.println(daba);
		}
		writer.close();
		System.exit(0);
	}
	private static Set<String> closure(Set<String> core, Set<FD> fds) {
		Set<String> closed = new HashSet<String>();
//		if(core.size()==1) {
//			System.out.println(core.toString() + "we are abse case" + baseclosure(core, fds));
//			return baseclosure(core, fds);
//		}
		closed.addAll(core);
		if(core.size()>0) {
			int closed_size_init = 0;
			while(closed.size() > closed_size_init) {	
				int f3 = 0;
				closed_size_init = closed.size();
				for(int size = 1; size <= closed.size(); size++) {
					for(Set<String> combination:Sets.combinations(closed, size)) {
						closed.addAll(baseclosure(combination, fds));
						if(closed.size()> closed_size_init) {
							f3 = 1;
							break;
						}
					}
					if(f3 == 1) {
						break;
					}
				}
			}
		}
		return closed;
	}
	private static Set<String> baseclosure(Set<String> core, Set<FD> fds) {
		Set<String> closed = new HashSet<String>();
		closed.addAll(core);
		int f1 = 0;
		for(FD f:fds) {
		if(f.getLHS().size() == core.size()) {
			for(String s: f.getLHS()) {
				if(!core.contains(s)) {
					f1 = 1;
					break;
				}
			}
			if(f1 == 1) {
				f1 = 0;
				continue;
			}
			else {
//				System.out.println(f.getLHS());
				for(String s: f.getRHS())
					closed.add(s);
			}
		}
		}
		return closed;
	}
}
