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

package deidentification.mcw;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import deidentification.DeidentificationRegex;
import deidentification.MedicalRecordWrapper;
import deidentification.NamedEntityRecognition;
import util.SimpleIRUtilities;

public class DeidServiceMethods {
	
	private static final long serialVersionUID = 1L;
//	private static final Logger LOGGER = Logger.getLogger(DeidServiceServlet.class);
	private static final NumberFormat formatter = new DecimalFormat("#0.00000");
	
	private static NamedEntityRecognition namedEntityRecognition = null;
	private static DeidentificationRegex deidentificationRegex = null;
	
	private static Map<String, String> whiteListMap = new HashMap<String, String>();
	private static Map<String, String> blackListMap = new HashMap<String, String>();
	
	public static NamedEntityRecognition getNamedEntityRecognition() {
		return namedEntityRecognition;
	}

	public static void setNamedEntityRecognition(NamedEntityRecognition namedEntityRecognition) {
		DeidServiceMethods.namedEntityRecognition = namedEntityRecognition;
	}

	public static DeidentificationRegex getDeidentificationRegex() {
		return deidentificationRegex;
	}

	public static void setDeidentificationRegex(DeidentificationRegex deidentificationRegex) {
		DeidServiceMethods.deidentificationRegex = deidentificationRegex;
	}

	public static Map<String, String> getWhiteListMap() {
		return whiteListMap;
	}

	public static void setWhiteListMap(Map<String, String> whiteListMap) {
		DeidServiceMethods.whiteListMap = whiteListMap;
	}

	public static Map<String, String> getBlackListMap() {
		return blackListMap;
	}

	public static void setBlackListMap(Map<String, String> blackListMap) {
		DeidServiceMethods.blackListMap = blackListMap;
	}

	public static NumberFormat getNumberFormatter() {
		return formatter;
	}
	
	public static String deidService(String id, String note_id, String name, int dateoffset, String text) throws Exception {
		
		StringBuffer sbb = new StringBuffer();
		MedicalRecordWrapper record = null;
		long start = System.currentTimeMillis();

		if (text != null && text.trim().length() > 0) {
			try {
                java.util.Date startDateRegex = new java.util.Date();
                String preprocessedText = deidentificationRegex.compositeRegex(text, dateoffset, blackListMap, name);
                java.util.Date endDateRegex = new java.util.Date();
                record = new MedicalRecordWrapper(id, note_id, text, preprocessedText, null, dateoffset, name);
                long msecs = SimpleIRUtilities.getElapsedTimeMilliseconds(startDateRegex, endDateRegex);
                record.setMillisecondsRegex(msecs);
                record.setDeIdText( namedEntityRecognition.performAnnotation( record.getRegexText() ) );
				String elapsed = getNumberFormatter()
						.format((System.currentTimeMillis() - start) / 1000d);
				System.out.println(record);
				System.out.println(record.getDeIdText());
				System.out.println("" + elapsed + " msecs");
			} catch (Exception e) {
				throw new Exception(e);
			}
		}
		return record.getDeIdText();
	}
	
	public void init(String blacklistfilename, String namedentityrecognitionclass, String regexdeidentificationclass)  throws Exception {

        String[] whiteListArray = null;
        String[] blackListArray = null;
        
    	ClassLoader classLoader = getClass().getClassLoader();
    	File backlistfile = new File(classLoader.getResource(blacklistfilename).getFile());

        //////////////////////////////////////////////////
        // read white list - pass through list
//        File whitelistfile = new File(whitelistfilename);
//        File backlistfile = new File(blacklistfilename);
//        try {
//            whiteListArray = loadFileList(whitelistfile);
//        } catch (IOException e1) {
//            e1.printStackTrace();
//        }

        // read black list - block list
        try {
            blackListArray = loadFileList(backlistfile);
        } catch (IOException e2) {
            e2.printStackTrace();
        }

        whiteListMap = new HashMap<String, String>();
        blackListMap = new HashMap<String, String>();

        if (whiteListArray != null && whiteListArray.length > 0) {
            for (int i = 0; i < whiteListArray.length; i++) {
                String[] stringArray = whiteListArray[i].split("\\s+");
                for (int j = 0; j < stringArray.length; j++) {
                    String s = stringArray[j].toLowerCase();
                    whiteListMap.put(s, s);
                }
            }
        }
        if (blackListArray != null && blackListArray.length > 0) {
            for (int i = 0; i < blackListArray.length; i++) {
                String[] stringArray = blackListArray[i].split("\\s+");
                for (int j = 0; j < stringArray.length; j++) {
                    String s = stringArray[j].toLowerCase();
                    blackListMap.put(s, s);
                }
            }
        }
        
        try {
            namedEntityRecognition = (NamedEntityRecognition) Class.forName(namedentityrecognitionclass).newInstance();
            System.out.println("CLASS TO DEID WITH : " + namedentityrecognitionclass);
            deidentificationRegex = (DeidentificationRegex) Class.forName(regexdeidentificationclass).newInstance();
        } catch (Exception e1) {
            new Exception(e1);
        }

        namedEntityRecognition.setWhiteListMap(whiteListMap);
        namedEntityRecognition.setBlackListMap(blackListMap);
	}
	
    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return encoding.decode(ByteBuffer.wrap(encoded)).toString();
    }

    static String[] loadFileList(File file) throws IOException {

        List<String> list = new ArrayList<String>();
        BufferedReader in = new BufferedReader(new FileReader(file));
        String str;
        while ((str = in.readLine()) != null) {
            String s = str.trim();
            if (s.length() > 0) {
                s = str.toLowerCase(); // normalize to upper case
                list.add(s);
            }
        }
        return list.toArray(new String[list.size()]);
    }
	
	public static void main(String[] args) {
		
//        String whitelistfilename = getServletContext().getRealPath( config.getInitParameter("whitelistfilename") );
//        String blacklistfilename = getServletContext().getRealPath( config.getInitParameter("blacklistfilename") );
//        String namedentityrecognitionclass = config.getInitParameter("namedentityrecognitionclass");
//        String regexdeidentificationclass = config.getInitParameter("regexdeidentificationclass");
        
		if( args.length != 5) {
			System.out.println("args: inputfile outputfile blacklistfilename namedentityrecognitionclass regexdeidentificationclass");
			System.exit(0);
		}
		String inputfile = args[0];
		String outputfile = args[1];
//		String whitelistfilename = null;
		String blacklistfilename = args[2];
		String namedentityrecognitionclass = args[3];
		String regexdeidentificationclass = args[4];
		
		DeidServiceMethods deidServiceMethods = new DeidServiceMethods();
		try {
			deidServiceMethods.init(blacklistfilename, namedentityrecognitionclass, regexdeidentificationclass);
		} catch (Exception e) {
			e.printStackTrace();
		}
				
    	try {
            // File names for testing ... comment out args test below
//            String inputFilePath = "file://Users/jayurbain/Dropbox/MCW/data/inc_hno_note_2017_06022017_xx1.tsv";
//            String [] fileSplit = inputFilePath.split("\\.");
//            String outputFilePath = fileSplit[0] + "_deid." + fileSplit[1];
            
            // This will reference one line at a time
            String line = null;
            try {
            	Configuration conf = new Configuration();
            	FileSystem inputfs = FileSystem.get(URI.create(inputfile), conf);
        		FSDataInputStream inputStream = inputfs.open(new Path(inputfile));
            	FileSystem outputfs = FileSystem.get(URI.create(outputfile), conf);
            	Path path = new Path(outputfile);
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
                int count=0;
                while((line = br.readLine()) != null) {
                    System.out.println("***");
//                    System.out.println(line);
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
                 	 
//                     String deidText = deidServiceRequest(url, buildDeidRequest(name, date_shift, note));
                     String deidText = DeidServiceMethods.deidService("1", "n1", name, dateShift, note);

                	if( deidText != null) {
//                		System.out.println(deidText);
                		bw.write(PATIENT_NUM + "\t" + encounter_num + "\t" + CONTACT_DATE + "\t" + deidText + "\n");
                		bw.write(id + "\t" + deidText + "\n");
                		System.out.println("" + ++count);
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
