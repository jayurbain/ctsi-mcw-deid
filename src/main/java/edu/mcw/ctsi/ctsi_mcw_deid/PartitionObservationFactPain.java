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

public class PartitionObservationFactPain {
		
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
		            
	            	FileSystem outputfs = FileSystem.get(URI.create(outputFilePath), conf);
	            	
	            	// 2017
	            	Path path2017 = new Path(outputFilePath);
	            	if ( outputfs.exists( path2017 )) { outputfs.delete( path2017, true ); } 
	        		OutputStream outputStream2017 = outputfs.create( path2017 );
	                BufferedWriter bw2017 = new BufferedWriter(new OutputStreamWriter(outputStream2017));
	                
//	                fields = [StructField("ENCOUNTER_NUM", StringType(), True),
//	                      	StructField("PATIENT_NUM", StringType(), True),
//	                      	StructField("CONCEPT_CD", StringType(), True),
//	                      	StructField("PROVIDER_ID", StringType(), True),
//	                      	StructField("START_DATE", DateType(), True),
//	                      	StructField("MODIFIER_CD", StringType(), True)]

	                int records = 0;
	                while((line = br.readLine()) != null && (records < nrecords || nrecords <=0) ) {
//	                    System.out.println("***");
//	                	System.out.println(line);
	                    String [] tokens = line.split("\t");
//	                    String ENCOUNTER_NUM = tokens[0].trim();
//	                    String PATIENT_NUM = tokens[1].trim();
//	                    String CONCEPT_CD = tokens[2].trim();
	                    String PROVIDER_ID = tokens[3].trim();
	                    String START_DATE = tokens[4].trim();
//	                    String MODIFIER_CD = tokens[5].trim();
	                    
	                    if( 
	                    	PROVIDER_ID.matches("92790971f1d12405a89eafc23cbbb2ff23624bae3217f5e64608655721c2807e") ||
	                    	PROVIDER_ID.matches("70e8ed84d9d176d16413b27dff70b0f48256213e603ad04cfea49d1a4e122a21") ||
	                    	PROVIDER_ID.matches("5181fdd1cebb662b3c0923857f9f18b0d40cd9b4cb42844fc0fdb2b5fb1f2880") ||
	                    	PROVIDER_ID.matches("0cad405c2fdefe6c700a3e1d4a4758bcf2d86741ce001929d9f63c76083e9ce5") ||
	                    	PROVIDER_ID.matches("aee4e43e16ff4770701ad7dcf0133d9094b444330a05bc57a917bfc954f7e4a7") ||
	                    	PROVIDER_ID.matches("9a2e3648a299de0a0b9bfe9d3ec8f990f64ef6a6d891bee7f5cfedb8cac87912") ||
	                    	PROVIDER_ID.matches("387ca0ebc629838803051bc96a37d755c85b0d81130dbe73b67fd89558bbcbec") ||
	                    	PROVIDER_ID.matches("bd99a65797cf86036488b0ee78656d06ea68eccd44e2ef1ec3cd7b04dda3c9cb") ||
	                    	PROVIDER_ID.matches("8d37de0e665e1110b898d0163ffe19006a8cb14cfb10f4928339d9aa7c95d8ed")   )  {
	                    
		                    bw2017.write(line + "\n"); 
		                    
		                    records++;
		                    if( records % 10000 == 0 ) {
		                    	System.out.println(records);
		                    }
	                    }
	                }   

	                // Always close files.
	                br.close(); 
	                bw2017.close(); 
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