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

public class PartitionNotesIntoSeparateFiles {
		
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
	            	System.out.println("ERROR -- arguments: inputfile outputdirectory nrecords");
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

	                int records = 0;
	                while((line = br.readLine()) != null && (records < nrecords || nrecords <=0) ) {
//	                    System.out.println("***");
//	                	System.out.println(line);
	                    String [] tokens = line.split("\t");
	                    // if no PAT_ENC_CSN_ID, can't join
	                    String PATIENT_NUM = tokens[0].trim();
	                    String ENCOUNTER_NUM = tokens[0].trim();
	                    String CONTACT_DATE = tokens[2].trim();
	                    CONTACT_DATE = CONTACT_DATE.replaceAll("/", "-");
	                    String text = tokens[3].trim();

	                    // 2014-12-1
//	                    String [] dateTokens = CONTACT_DATE.split("-");
//	                    int year = Integer.parseInt( dateTokens[0]);
	                    
		                // output files
//			            fileSplit = outputFilePath.split("\\.");
//			            String filePostFix = "";
//			            if(fileSplit.length>1) {
//			            	filePostFix = "."+ fileSplit[1];
//			            }
	                    
	                    String outputFilePath_ = outputFilePath+"/"+PATIENT_NUM+"_"+ENCOUNTER_NUM+"_"+CONTACT_DATE+".txt";
		            	FileSystem outputfs = FileSystem.get(URI.create(outputFilePath_), conf);

		            	Path path = new Path(outputFilePath_);
		            	if ( outputfs.exists( path )) { outputfs.delete( path, true ); } 
		        		OutputStream outputStream = outputfs.create( path );
		                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
		                bw.write(text + "\n");
		                bw.flush(); 
		                bw.close(); 
		                
	                    outputFilePath_ = outputFilePath+"/"+PATIENT_NUM+"_"+ENCOUNTER_NUM+"_"+CONTACT_DATE+".ann";
		            	outputfs = FileSystem.get(URI.create(outputFilePath_), conf);

		            	path = new Path(outputFilePath_);
		            	if ( outputfs.exists( path )) { outputfs.delete( path, true ); } 
		        		outputStream = outputfs.create( path );
		                bw = new BufferedWriter(new OutputStreamWriter(outputStream));
		                bw.write("\n");
		                bw.flush(); 
		                bw.close(); 
		                records++;
	                }   
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