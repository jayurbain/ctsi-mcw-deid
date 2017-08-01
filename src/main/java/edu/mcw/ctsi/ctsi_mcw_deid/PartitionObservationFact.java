/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
    This file is part of "CTSI MCW NLP" for removing
    protected health information from medical records.

    "CTSI MCW NLP" is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    "CTSI MCW NLP" is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with "CTSI MCW NLP."  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * @author jayurbain, jay.urbain@gmail.com
 * 
 */

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

public class PartitionObservationFact {
		
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
	                    String CONCEPT_CD = tokens[2].trim();
//	                    String PROVIDER_ID = tokens[3].trim();
	                    String START_DATE = tokens[4].trim();
//	                    String MODIFIER_CD = tokens[5].trim();
	                    
//	                    select * from observation_fact where concept_cd like 'ICD9%';
//	                    select * from observation_fact where concept_cd like 'CPT%';
//	                    select * from observation_fact where concept_cd like 'LOINC%';
//	                    select * from observation_fact where concept_cd like 'ORDER_MED_ID%';
	                    
	                    if( CONCEPT_CD.matches("^ICD.*|^CPT.*|^LOINC.*|^ORDER_MED_ID.*") == false ) {
	                    	continue;
	                    }
	                    
	                    // 2014-12-1
	                    String [] dateTokens = START_DATE.split("-");
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
	                    if( records % 10000 == 0 ) {
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