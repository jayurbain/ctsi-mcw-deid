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

import org.apache.commons.httpclient.HttpStatus;
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
import org.json.JSONException;
import org.json.JSONObject;

public class Deid {
	
	// local deid service url
	static String url = "http://localhost:8080/ctsi-mcw-deid-service/deidservice";
	
	// sample data
	static String data = "{\"dateoffset\":\"10\",\"name\":\"Mel\",\"recordlist\":[\"Jay Urbain, jay.urbain@gmail.com, born December 6, 2156 is an elderly caucasian male suffering from illusions of grandeur and LBP.\", \"He is married to Kimberly Urbain, who is much better looking.\", \"Patient father, Francis Urbain has a history of CAD and DM.\", \"Jay has been prescribed meloxicam, and venti americano.\", \"He lives at 9050 N. Tennyson Dr., Disturbia, WI with his wife and golden retriever Mel.\", \"You can reach him at 414-745-5102.\"]}";
	static String sampleName = "Jay Urbain";
	static String sampleDateOffset = "-10";
	static String sampleText = "Jay Urbain, jay.urbain@gmail.com, born 'December 6, 2156' is an elderly caucasian male suffering from illusions of grandeur and LBP. He is married to Kimberly Urbain, who is much better looking. Patient father, Francis Urbain has a history of CAD and DM. Jay has been prescribed meloxicam, and venti americano. He lives at 9050 N. Tennyson Dr., Disturbia, WI with his wife and golden retriever Mel. You can reach him at 414-745-5102.";
	
	public static String buildDeidRequest(String name, String dateoffset, String text) {
//		String t = text.replace("'", "");
//		t = text.replace("\"", "");
//		t = text.replace("\\", "");
//		String t = text.replaceAll("'|\"|\\\\","");
				
		String t = text.replace("\\", "");;
		t = org.json.simple.JSONObject.escape(t);
		String data = "{\"dateoffset\":\"" + dateoffset + 
						"\",\"name\":\"" + name + 
						"\"," + "\"recordlist\":[\"" + t + "\"]}";
		return data;
	}
	
	public static String deidServiceRequest(String url, String data) {
		
		HttpClient httpClient = HttpClientBuilder.create().build(); //Use this instead 
		String text = null;
		
	    try {
			HttpPost request = new HttpPost(url);
			StringEntity params =new StringEntity(data);
			request.addHeader("content-type", "application/x-www-form-urlencoded");
			request.setEntity(params);
			HttpResponse response = httpClient.execute(request);
			System.out.println(response);
			HttpEntity entity = response.getEntity();
			if( response.getStatusLine().getStatusCode() == HttpStatus.SC_OK ) {
				String responseString = EntityUtils.toString(entity, "UTF-8");
				System.out.println(responseString);
				
			    // build a JSON object
			    JSONObject obj = null;
				try {
					obj = new JSONObject(responseString);

				    String name = obj.getString("name");
				    String dateoffset = obj.getString("dateoffset");
				    JSONArray deidlistJSONArray = obj.getJSONArray("deidlist");
				    ArrayList<String> deidList = new ArrayList<String>();
				    StringBuffer sb = new StringBuffer();
				    for(int i=0; i<deidlistJSONArray.length(); i++) {
				    	sb.append( deidlistJSONArray.get(i).toString() );
				    }
				    text = sb.toString();
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
			else {
				System.out.println("ERROR *** Unable to process: " + data);
			}
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    return text;
	}
	
    public static void main( String[] args ) {
        
    	try {
            // File names for testing ... comment out args test below
            String inputFilePath = "file://Users/jayurbain/Dropbox/MCW/data/inc_hno_note_2017_06022017_xx1.tsv";
            String [] fileSplit = inputFilePath.split("\\.");
            String outputFilePath = fileSplit[0] + "_deid." + fileSplit[1];
            
            if(args.length >= 2) {
            	inputFilePath = args[0];
            	outputFilePath = args[1];
                if(args.length == 3) {
                	url = args[2];
                }
            }
            else {
            	System.out.println("ERROR -- arguments: inputfile outputfile");
            	System.exit(-1);
            }
            
            // This will reference one line at a time
            String line = null;
            try {
            	Configuration conf = new Configuration();
            	FileSystem inputfs = FileSystem.get(URI.create(inputFilePath), conf);
        		FSDataInputStream inputStream = inputfs.open(new Path(inputFilePath));
            	FileSystem outputfs = FileSystem.get(URI.create(outputFilePath), conf);
            	Path path = new Path(outputFilePath);
            	if ( outputfs.exists( path )) { outputfs.delete( path, true ); } 
        		OutputStream outputStream = outputfs.create( path );
          
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
           
//                fields = [StructField("PAT_ID", StringType(), True),
//                      	StructField("PAT_ENC_CSN_ID", StringType(), True),
//                      	StructField("CONTACT_DATE", DateType(), True),
//                      	StructField("IP_NOTE_TYPE_C", StringType(), True),
//                      	StructField("NOTE_TEXT", StringType(), True)]
                
                // id             first_name            last_name            date_shift             note
//                line = br.readLine(); // header
                while((line = br.readLine()) != null) {
                    System.out.println("***");
                    System.out.println(line);
                    String [] tokens = line.split("\t");
//                    String PAT_ID = tokens[0].trim();
//                    String PAT_ENC_CSN_ID = tokens[1].trim();
//                    String CONTACT_DATE = tokens[2].trim();
//                    String IP_NOTE_TYPE_C = tokens[3].trim();
//                    String NOTE_TEXT = tokens[4].trim();
//                    String id = PAT_ID;
//                    String deidText = deidServiceRequest(url, buildDeidRequest(sampleName, sampleDateOffset, NOTE_TEXT));
                    
//                    String id = tokens[0].trim();
//                    String first_name = tokens[1].trim();
//                    String last_name = tokens[2].trim();
//                    String name = first_name + " " + last_name;
//                    String date_shift = tokens[3].trim();
//                    int dateShift = Integer.parseInt(date_shift);
//                    String note = tokens[4].trim();
//                	String deidText = deidServiceRequest(url, buildDeidRequest(name, date_shift, note));
                	
                	 //PAT_ID|PAT_ID_ENC|DATE_OFF|PATIENT_NUM|PAT_ENC_CSN_ID|encounter_num|pat_first_name|pat_last_name|pat_middle_name|  PAT_ID|PAT_ENC_CSN_ID|CONTACT_DATE|IP_NOTE_TYPE_C|           NOTE_TEXT
                     String id = tokens[0].trim();
                     String PAT_ID_ENC = tokens[1].trim();
                     String date_shift = tokens[2].trim();
                     int dateShift = Integer.parseInt(date_shift);
                     String PATIENT_NUM = tokens[3].trim();
                     String PAT_ENC_CSN_ID = tokens[4].trim();
                     String encounter_num = tokens[5].trim();
	                 String pat_first_name = tokens[6].trim();
	                 String pat_last_name = tokens[7].trim();
	                 String name = pat_first_name + " " + pat_last_name;
                     
                     String CONTACT_DATE = tokens[11].trim();
                     String IP_NOTE_TYPE_C = tokens[12].trim();
                     String note = tokens[13].trim();
                 	 
                     String deidText = deidServiceRequest(url, buildDeidRequest(name, date_shift, note));
                 	
                	if( deidText != null) {
                		System.out.println(deidText);
                		bw.write(PATIENT_NUM + "\t" + encounter_num + "\t" + CONTACT_DATE + "\t" + deidText + "\n");
                		bw.write(id + "\t" + deidText + "\n");
                	}
                }   

                // Always close files.
                br.close(); 
                bw.close(); 
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
