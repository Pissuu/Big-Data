package edu.rit.ibd.a3;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
public class CandidateKeyDiscovery {
	public static void main(String[] args) throws Exception {
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
//		final String relation = "class (course, title, department, credits, section, semester, year, building, room, capacity)";
//		final String fdsStr = "course -> title, department, credits ; building, room -> capacity ; course, section, semester, year -> building, room";
//		final String outputFile = "output.txt";
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input.
		Set<FD> fds = new HashSet<>();
		// This stores the candidate keys discovered; each key is a set of attributes.
		List<Set<String>> ck = new ArrayList<>();
		
		// TODO 0: Your code here!
		
		// Parse the input relation that include its attributes. Recall that relation and attribute names can be formed by multiple letters.
		//
		// Parse the input functional dependencies. Recall that attributes can be formed by multiple letters.
		//
		// For each attribute a, you must classify as case 1 (a is not in the functional dependencies), case 2 (a is only in the right-hand side),
		//	case 3 (a is only in the left-hand side), case 4 (a is in both left- and right-hand sides).
		//
		// Compute the core (cases 1 and 3) and check whether the core is candidate key based on closure.
		//
		// If the closure of the core does not contain all the attributes, proceed to combine attributes.
		//
		// For each combination of attributes starting from size 1 classified as case 4:
		//	X = comb union core
		//	If the closure of X contains all attributes of the input relation:
		//		X is superkey
		//		If X is not contained in a previous candidate key already discovered:
		//			X is a candidate key
		//	If all the combinations of size k are superkeys -> Stop
		
		//Parse attributes from the input relation r(a,b,c,f,e)
		String rel = relation;
		rel = rel.substring(rel.indexOf("(")+1);
		rel = rel.substring(0,rel.indexOf(")"));
		String alph[] = rel.split(",");
		for(String i : alph)
			attributes.add(i.trim());
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
		Set<String> fds_alph = new HashSet<String>();
		for(FD fd: fds) {
			for(String rhs : fd.getRHS()) 
				fds_alph.add(rhs);
			for(String lhs : fd.getLHS()) 
				fds_alph.add(lhs);
		}
			
//		System.out.println("The elements in the FDs are: " + Arrays.toString(fds_alph.toArray()));
		//case 1,2,3,4
		Set<String> case1 = new HashSet<>(), case2 = new HashSet<>(), case3 = new HashSet<>(), case4 = new HashSet<>();
		for(String r: alph) 
			if(!fds_alph.contains(r.trim()))
				case1.add(r.trim());
		
		Set<String> RHS = new HashSet<>();
		Set<String> LHS = new HashSet<>();
		for(FD o: fds) {
			for(String r: o.getRHS())
				RHS.add(r.trim());
			for(String l:o.getLHS())
				LHS.add(l.trim());
		}
		
		for(String r: RHS)
			if(LHS.contains(r))
				case4.add(r.trim());
			else
				case2.add(r);
		for(String l: LHS)
			if(!RHS.contains(l))
				case3.add(l.trim());
		
		System.out.println("case1: "+case1.toString());
		System.out.println("case2: "+case2.toString());
		System.out.println("case3: "+case3.toString());
		System.out.println("case4: "+case4.toString());
//			
		Set<String> core = new HashSet<>();
		core.addAll(case1);
		core.addAll(case3);
		//check if core is empty, then directly skip this step or smtn
//		System.out.println("closure of " + core.toString() + "returns " + closure(core, fds).toString());
		if(closure(core, fds).containsAll(attributes) && core.size()>0) {
			ck.add(core);
		}
		else {
			List<Set<String>> ck_old = new ArrayList<>();
			ck_old.addAll(ck);
			int flag = 0;
			for(int size = 1; size <= case4.size(); size++) {
				ck_old.addAll(ck);
				for(Set<String> combination:Sets.combinations(case4, size)) {
					Set<String> candidateAttributes = new HashSet<>();
					candidateAttributes.addAll(core);
					candidateAttributes.addAll(combination);
//					System.out.println(candidateAttributes);
					for(Set<String> keys: ck_old) {
							if(candidateAttributes.containsAll(keys)) {
								flag = 1;
//								System.out.println("here" + candidateAttributes + keys.toString());
								break;
							}
					}
					if(flag == 1 && candidateAttributes.size()!=1 && ck_old.size()!=0) {
//						System.out.println(combination + ck_old.toString());
						flag = 0;
						continue;
					}
					else {
//						System.out.println(combination + ck_old.toString());
//						System.out.println("combination we are taking is " + combination + "against " + ck_old.toString());
						System.out.println("closure of " + candidateAttributes.toString() + "returns " + closure(candidateAttributes, fds).toString());
						Set<String> temp = new HashSet<>();
						temp = closure(candidateAttributes, fds);
						int f2 = 0;
						if(temp.size() == attributes.size()) {
							ck.add(candidateAttributes);
							System.out.println("here ck is " + combination + " and returns " + ck.toString());
						}
					} 
						
					//Minimality. if there is a previous candidate key that contains all these attributes
					//closure and add to keys.
				}
			}
//			System.exit(0);
		}
		System.out.println(ck.toString());
			
		PrintWriter writer = new PrintWriter(new File(outputFile));
		for (Set<String> key : ck)
			writer.println(key.stream().sorted().collect(java.util.stream.Collectors.toList()).
					toString().replace("[", "").replace("]", ""));
		writer.close();
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
