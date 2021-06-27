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
public class CanonicalCoverComputation {
	
	public static void main(String[] args) throws Exception {
		// This stores the candidate keys discovered; each key is a set of attributes.
		final String relation = args[0];
		final String fdsStr = args[1];
		final String outputFile = args[2];
//		final String relation = "r(A,B,C,D,E,G,H,K)";
//		final String fdsStr = "A,B,H->C;A->D,E;B,G,H->K;K->A,D,H;B,H->G,E";
//		final String outputFile = "output.txt";
		// This stores the attributes of the input relation.
		Set<String> attributes = new HashSet<>();
		// This stores the functional dependencies provided as input. This will be the output as well.
		Set<FD> fds = new HashSet<>();
		
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
			fds = union(fds);
				
		
	int	removing = 1;
	int flag = 0;
	while(removing == 1) {
		removing = 0;
		for(FD fd:fds) {
			if(fd.getLHS().size()==1) 
				continue;
			for(String a: fd.getLHS()) {
				Set<FD> fds2 = new HashSet<>();
				fds2.addAll(fds);
//				System.out.println("LHS: can we remove " + a + " from : " + fd.getLHS() + " -> " + fd.getRHS()+ " in " + fds2.toString());
				Set<String> temp1 = new HashSet<String>();
				Set<String> temp2 = new HashSet<String>();
				temp1.addAll(fd.getRHS());
				temp2.addAll(fd.getLHS());
				temp2.remove(a);
				if(closure(temp2, fds).containsAll(temp1)) {
					removing = 1;
					flag = 1;
//					System.out.println("yes we can " );
					FD fd1 =new FD();
					for(FD s: fds2) 
						if(s.getLHS()==fd.getLHS()) {
							fds2.remove(s);
							break;
						}
					fd1.setLHS(temp2);
					fd1.setRHS(fd.getRHS());
					fds2.add(fd1);
					fds2 = union(fds2);
					fds.clear();
					fds.addAll(fds2);
				}
//				else {
//					System.out.println("condition: " + closure(temp2, fds).toString() + " contains " + temp1.toString() + closure(temp2, fds).containsAll(temp1));
//					System.out.println(closure(temp2, fds).toString());
//					System.out.println(temp1.toString());
//					for(String q:closure(temp2, fds)) {
//						System.out.println(q);
//						System.out.println(temp1.toString());
//						if(q==temp1.toString())
//							System.out.println("aldfjasd");
//					}
//				}
				if(flag == 1)
					break;
				}
			if(flag == 1) {
				flag = 0;
				break;
				}
		}

		for(FD fd:fds) {
			if(fd.getRHS().size()==1)
				continue;
			Set<FD> fds1 = new HashSet<>();
			fds1.addAll(fds);
			for(String a: fd.getRHS()) {
				Set<FD> fds2 = new HashSet<>();
				fds2.addAll(fds);
//				System.out.println("RHS: can we remove " + a + " from : " + fd.getLHS() + " -> " + fd.getRHS() + " in " + fds2.toString());
				for(FD s2: fds1)
					if(s2.getLHS()==fd.getLHS()) {
						fds1.remove(s2);
						break;
					}
				FD fd1 = new FD();
				fd1.setLHS(fd.getLHS());
				Set<String> r = new HashSet<String>();
				r.addAll(fd.getRHS());
				r.remove(a);
				fd1.setRHS(r);
				fds1.add(fd1);
				Set<String> temp1 = new HashSet<>();
				System.out.println("the updated F':" + fds1.toString());
				if(closure(fd.getLHS(), fds1).contains(a)) {
					removing = 1;
					flag = 1;
					for(FD f1: fds2) 
						if(f1.getLHS()==fd.getLHS()) {
							fds2.remove(f1);
							break;
						}
					FD fd2 = new FD();
					temp1.addAll(fd.getRHS());
					temp1.remove(a);
					fd2.setRHS(temp1);
					fd2.setLHS(fd.getLHS());
					fds2.add(fd2);
//					System.out.println("yes we can " );
					System.out.println(fds2.toString());
					fds2 = union(fds2);
					fds.clear();
					fds.addAll(fds2);
				}
				if(flag == 1)
					break;
			}
			if(flag == 1) {
				flag = 0;
				break;
			}
		}

	}
		PrintWriter writer = new PrintWriter(new File(outputFile));
//		for (FD fd : fds) {
//			String b="";
//			for(String a: fd.getRHS())
//				b = b + a + ", ";
//			b = b.substring(0,b.length()-2);
//			String c="";
//			for(String a: fd.getLHS())
//				c = c + a + ", ";
//			c = c.substring(0,c.length()-2);
//			System.out.println(c + " -> " + b);
//			writer.println(c + " -> " + b);
//		}
		for(FD i: fds) {
			String[] temp1 = new String[i.getLHS().size()];
			String[] temp2 = new String[i.getRHS().size()];
			int count = 0;
			for(String j: i.getLHS()) {
				temp1[count] = j;
				count = count + 1;
			}
			count = 0;
			for(String j: i.getRHS()) {
				temp2[count] = j;
				count = count + 1;
			}
			Arrays.sort(temp1);
			String temp11 =String.join(", ", temp1);
			Arrays.sort(temp2);
			String temp12 =String.join(", ", temp2);
			writer.println(temp11 + " -> " + temp12);
			System.out.println(temp11 + " -> " + temp12);
		}
		writer.close();
	}
		private static Set<FD> union(Set<FD> fds) {
			Set<Set<String>> lhs_only = new HashSet<>();
			for(FD f: fds) {
//				System.out.println("we are here");
				lhs_only.add(f.getLHS());
			}
			Set<FD> fds1 = new HashSet<>();
			for(Set<String> lhs: lhs_only) {
				if(lhs == null)
					continue;
				List<Set<String>> rhs_only = new ArrayList<>();
				for(FD f: fds) {
					if(f.getLHS()==null)
						continue;
					if(f.getLHS().equals(lhs))
						rhs_only.add(f.getRHS());
				}
				FD f1 = new FD();
				f1.setLHS(lhs);
				Set<String> temp = new HashSet<String>();
				for(Set<String> rh: rhs_only)
					temp.addAll(rh);
				f1.setRHS(temp);
				fds1.add(f1);
			}
			return fds1;
	}
		private static Set<String> closure(Set<String> core, Set<FD> fds) {
			Set<String> closed = new HashSet<String>();
//			if(core.size()==1) {
//				System.out.println(core.toString() + "we are abse case" + baseclosure(core, fds));
//				return baseclosure(core, fds);
//			}
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
//					System.out.println(f.getLHS());
					for(String s: f.getRHS())
						closed.add(s);
				}
			}
			}
			return closed;
		}
}
