package edu.rit.ibd.a1;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class test{
	public static void main(String[] args) throws FileNotFoundException, IOException {
		String folderToIMDBGZipFiles = "C:/Users/jalaja/Desktop/2nd_Semester/data/";//folder to imdb zip files
		int countLines = 0;
		InputStream gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.basics.tsv.gz"));
		System.out.println("The file name is : title basics");
		Scanner sc = new Scanner(gzipStream, "UTF-8");
//		sc.nextLine();
		while (sc.hasNextLine() & countLines <= 100) {
			String currentLine = sc.nextLine();
			countLines++;
			if(countLines <= 100)
				System.out.println(currentLine);
			}
		sc.close();
		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"name.basics.tsv.gz"));
		System.out.println("The file name is : name basics");
		sc = new Scanner(gzipStream, "UTF-8");
		while (sc.hasNextLine() & countLines <= 100) {
			String currentLine = sc.nextLine();
			countLines++;
			if(countLines <= 100)
				System.out.println(currentLine);
			}
		sc.close();
		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.crew.tsv.gz"));
		System.out.println("The file name is : title crew");
		sc = new Scanner(gzipStream, "UTF-8");
		while (sc.hasNextLine() & countLines <= 500) {
			String currentLine = sc.nextLine();
			countLines++;
			if(countLines <= 500)
				System.out.println(currentLine);
			}
		sc.close();
		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.principals.tsv.gz"));
		System.out.println("The file name is : title principals");
		sc = new Scanner(gzipStream, "UTF-8");
		while (sc.hasNextLine() & countLines <= 1000) {
			String currentLine = sc.nextLine();
			countLines++;
			if(countLines <= 1000)
				System.out.println(currentLine);
			}
		sc.close();
		countLines = 0;
		gzipStream = new GZIPInputStream(new FileInputStream(folderToIMDBGZipFiles+"title.ratings.tsv.gz"));
		System.out.println("The file name is : title ratings");
		sc = new Scanner(gzipStream, "UTF-8");
		while (sc.hasNextLine() & countLines <= 100) {
			String currentLine = sc.nextLine();
			countLines++;
			if(countLines <= 100)
				System.out.println(currentLine);
			}
		sc.close();
		}
	}
