package edu.mcw.ctsi.ctsi_mcw_deid;

	import java.io.BufferedReader;
	import java.io.BufferedWriter;
	import java.io.FileNotFoundException;
	import java.io.FileReader;
	import java.io.FileWriter;
	import java.io.IOException;
	import java.io.InputStreamReader;
	import java.io.OutputStream;
	import java.io.OutputStreamWriter;
	import java.io.UnsupportedEncodingException;
	import java.net.URLEncoder;
	import java.util.ArrayList;
	import java.util.Scanner;
	import java.net.URI;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.FSDataInputStream;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.util.Progressable;
	import org.apache.http.HttpEntity;
	import org.apache.http.HttpResponse;
	import org.apache.http.client.ClientProtocolException;
	import org.apache.http.client.HttpClient;
	import org.apache.http.client.methods.HttpPost;
	import org.apache.http.entity.StringEntity;
	import org.apache.http.impl.client.HttpClientBuilder;
	import org.apache.http.util.EntityUtils;
	import org.json.JSONArray;
	import org.json.JSONObject;

public class PartitionNoteText {
		
		// sample data
		static String data = "{\"dateoffset\":\"10\",\"name\":\"Mel\",\"recordlist\":[\"Jay Urbain, jay.urbain@gmail.com, born December 6, 2156 is an elderly caucasian male suffering from illusions of grandeur and LBP.\", \"He is married to Kimberly Urbain, who is much better looking.\", \"Patient father, Francis Urbain has a history of CAD and DM.\", \"Jay has been prescribed meloxicam, and venti americano.\", \"He lives at 9050 N. Tennyson Dr., Disturbia, WI with his wife and golden retriever Mel.\", \"You can reach him at 414-745-5102.\"]}";
		static String sampleName = "Jay Urbain";
		static String sampleDateOffset = "-10";
		static String sampleText = "Jay Urbain, jay.urbain@gmail.com, born December 6, 2156 is an elderly caucasian male suffering from illusions of grandeur and LBP. He is married to Kimberly Urbain, who is much better looking. Patient father, Francis Urbain has a history of CAD and DM. Jay has been prescribed meloxicam, and venti americano. He lives at 9050 N. Tennyson Dr., Disturbia, WI with his wife and golden retriever Mel. You can reach him at 414-745-5102.";
		
		public static String buildDeidRequest(String name, String dateoffset, String text) {
			String data = "{\"dateoffset\":\"" + dateoffset + 
							"\",\"name\":\"" + name + 
							"\"," + "\"recordlist\":[\"" + text + "\"]}";
			return data;
		}
		
	    public static void main( String[] args ) {
	        
	    	try {
	            // File names for testing ... comment out args test below
	            String inputFilePath = "file://Users/jayurbain/Dropbox/MCW/data/inc_hno_note_2017_06022017_xx1.tsv";
	            String [] fileSplit = inputFilePath.split("\\.");
	            String outputFilePath = fileSplit[0] + "_deid." + fileSplit[1];
	            int nrecords = 0;
	            
	            if(args.length == 3) {
	            	inputFilePath = args[0];
	            	outputFilePath = args[1];
	            	nrecords = Integer.parseInt( args[2] );
	            }
	            else {
	            	System.out.println("ERROR -- arguments: inputfile outputfile nrecords");
	            	System.exit(-1);
	            }
	            
	            // This will reference one line at a time
	            String line = null;
	            try {
	            	Configuration conf = new Configuration();
	            	
	            	//input file
	            	FileSystem inputfs = FileSystem.get(URI.create(inputFilePath), conf);
	        		FSDataInputStream inputStream = inputfs.open(new Path(inputFilePath));
	                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
	                
	                // output files
		            fileSplit = outputFilePath.split("\\.");
		            String filePostFix = "";
		            if(fileSplit.length>1) {
		            	filePostFix = "."+ fileSplit[1];
		            }
		            String outputFilePath2017 = fileSplit[0] + "_" + 2017 + filePostFix;
		            String outputFilePath2016 = fileSplit[0] + "_" + 2016 + filePostFix;
		            String outputFilePath2015 = fileSplit[0] + "_" + 2015 + filePostFix;
		            String outputFilePath2014 = fileSplit[0] + "_" + 2014 + filePostFix;
		            String outputFilePath2013 = fileSplit[0] + "_" + 2013 + filePostFix;
		            String outputFilePath2012 = fileSplit[0] + "_" + 2012 + filePostFix;
		            String outputFilePath2011 = fileSplit[0] + "_" + 2011 + filePostFix;
		            String outputFilePath2010 = fileSplit[0] + "_" + 2010 + "older" + filePostFix;
		            
	            	FileSystem outputfs = FileSystem.get(URI.create(outputFilePath), conf);
	            	
	            	// 2017
	            	Path path2017 = new Path(outputFilePath2017);
	            	if ( outputfs.exists( path2017 )) { outputfs.delete( path2017, true ); } 
	        		OutputStream outputStream2017 = outputfs.create( path2017 );
	                BufferedWriter bw2017 = new BufferedWriter(new OutputStreamWriter(outputStream2017));
	            	
	            	// 2016
	            	Path path2016 = new Path(outputFilePath2016);
	            	if ( outputfs.exists( path2016 )) { outputfs.delete( path2016, true ); } 
	        		OutputStream outputStream2016 = outputfs.create( path2016 );
	                BufferedWriter bw2016 = new BufferedWriter(new OutputStreamWriter(outputStream2016));
	            	
	            	// 2015
	            	Path path2015 = new Path(outputFilePath2015);
	            	if ( outputfs.exists( path2015 )) { outputfs.delete( path2015, true ); } 
	        		OutputStream outputStream2015 = outputfs.create( path2015 );
	                BufferedWriter bw2015 = new BufferedWriter(new OutputStreamWriter(outputStream2015));

	            	// 2014
	            	Path path2014 = new Path(outputFilePath2014);
	            	if ( outputfs.exists( path2014 )) { outputfs.delete( path2014, true ); } 
	        		OutputStream outputStream2014 = outputfs.create( path2014 );
	                BufferedWriter bw2014 = new BufferedWriter(new OutputStreamWriter(outputStream2014));
	                
	            	// 2013
	            	Path path2013 = new Path(outputFilePath2013);
	            	if ( outputfs.exists( path2013 )) { outputfs.delete( path2013, true ); } 
	        		OutputStream outputStream2013 = outputfs.create( path2013 );
	                BufferedWriter bw2013 = new BufferedWriter(new OutputStreamWriter(outputStream2013));
	                
	            	// 2012
	            	Path path2012 = new Path(outputFilePath2012);
	            	if ( outputfs.exists( path2012 )) { outputfs.delete( path2012, true ); } 
	        		OutputStream outputStream2012 = outputfs.create( path2012 );
	                BufferedWriter bw2012 = new BufferedWriter(new OutputStreamWriter(outputStream2012));
	                
	            	// 2011
	            	Path path2011 = new Path(outputFilePath2011);
	            	if ( outputfs.exists( path2011 )) { outputfs.delete( path2011, true ); } 
	        		OutputStream outputStream2011 = outputfs.create( path2011 );
	                BufferedWriter bw2011 = new BufferedWriter(new OutputStreamWriter(outputStream2011));
	                
	            	// 2010
	            	Path path2010 = new Path(outputFilePath2010);
	            	if ( outputfs.exists( path2010 )) { outputfs.delete( path2010, true ); } 
	        		OutputStream outputStream2010 = outputfs.create( path2010 );
	                BufferedWriter bw2010 = new BufferedWriter(new OutputStreamWriter(outputStream2010));
	                
//	                fields = [StructField("PAT_ID", StringType(), True),
//	                      	StructField("PAT_ENC_CSN_ID", StringType(), True),
//	                      	StructField("CONTACT_DATE", DateType(), True),
//	                      	StructField("IP_NOTE_TYPE_C", StringType(), True),
//	                      	StructField("NOTE_TEXT", StringType(), True)]

	                int records = 0;
	                while((line = br.readLine()) != null && (records < nrecords || nrecords <=0) ) {
//	                    System.out.println("***");
//	                	System.out.println(line);
	                    String [] tokens = line.split("\t");
	                    // if no PAT_ENC_CSN_ID, can't join
	                    if( tokens[1] == null || tokens[1].length() == 0 ) {
	                    	continue;
	                    }
	                    String CONTACT_DATE = tokens[2].trim();

	                    // 2014-12-1
	                    String [] dateTokens = CONTACT_DATE.split("-");
	                    int year = Integer.parseInt( dateTokens[0]);
	                    
	                    switch(year)  {
	                    	case 2017: bw2017.write(line + "\n"); break;
	                    	case 2016: bw2016.write(line + "\n"); break;
	                    	case 2015: bw2015.write(line + "\n"); break;
	                    	case 2014: bw2014.write(line + "\n"); break;
	                    	case 2013: bw2013.write(line + "\n"); break;
	                    	case 2012: bw2012.write(line + "\n"); break;
	                    	case 2011: bw2011.write(line + "\n"); break;
	                    	default: bw2010.write(line + "\n");
	                    }
	                    records++;
	                    if( records % 100000 == 0 ) {
	                    	System.out.println(records);
	                    }
	                }   

	                // Always close files.
	                br.close(); 
	                bw2017.close(); 
	                bw2016.close(); 
	                bw2015.close(); 
	                bw2014.close(); 
	                bw2013.close(); 
	                bw2012.close(); 
	                bw2011.close(); 
	                bw2010.close(); 
	                System.out.println("records processed: " + records);
	            }
	            catch(FileNotFoundException ex) {
	            	 ex.printStackTrace();               
	            }
	            catch(IOException ex) {
	                ex.printStackTrace();
	            }
			} catch (Exception e) {
				e.printStackTrace();
			}
	    }

	}