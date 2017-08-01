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
/**
 *
 * MCW Deidentification - Regular expressions for email, web url's, addresses,
 * dates, phone numbers, dates, MRN - Stanford NLP named entity recognition
 * models for person identification using CRF (Conditional Random Field)
 * 2/14/2016
 */
package deidentification;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import deidentification.mcw.DeidServiceMethods;

//import deidentification.db.DBConnection;

//import deidentification.options.DeidOptions;
//import deidentification.options.cmdr.DeidOptionsJCmd;
//import deidentification.options.DeidOptionsParser;
import util.SimpleIRUtilities;


public class DeIdentification {

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
		DeIdentification.namedEntityRecognition = namedEntityRecognition;
	}

	public static DeidentificationRegex getDeidentificationRegex() {
		return deidentificationRegex;
	}

	public static void setDeidentificationRegex(DeidentificationRegex deidentificationRegex) {
		DeIdentification.deidentificationRegex = deidentificationRegex;
	}

	public static Map<String, String> getWhiteListMap() {
		return whiteListMap;
	}

	public static void setWhiteListMap(Map<String, String> whiteListMap) {
		DeIdentification.whiteListMap = whiteListMap;
	}

	public static Map<String, String> getBlackListMap() {
		return blackListMap;
	}

	public static void setBlackListMap(Map<String, String> blackListMap) {
		DeIdentification.blackListMap = blackListMap;
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
//		File backlistfile = new File(classLoader.getResource(blacklistfilename).getFile());
		File backlistfile = new File(blacklistfilename);

		//////////////////////////////////////////////////
		// read white list - pass through list
		//    File whitelistfile = new File(whitelistfilename);
		//    File backlistfile = new File(blacklistfilename);
		//    try {
		//        whiteListArray = loadFileList(whitelistfile);
		//    } catch (IOException e1) {
		//        e1.printStackTrace();
		//    }

		// read black list - block list
		try {
			blackListArray = loadFileList(backlistfile);
		} catch (IOException e2) {
			e2.printStackTrace();
		}

		whiteListMap = new HashMap<String, String>();
		blackListMap = new HashMap<String, String>();

		//		if (whiteListArray != null && whiteListArray.length > 0) {
		//			for (int i = 0; i < whiteListArray.length; i++) {
		//				String[] stringArray = whiteListArray[i].split("\\s+");
		//				for (int j = 0; j < stringArray.length; j++) {
		//					String s = stringArray[j].toLowerCase();
		//					whiteListMap.put(s, s);
		//				}
		//			}
		//		}
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

	/**
	 * @param args
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) {

		if( args.length < 7) {
			System.out.println("args: inputfile outputfile blacklistfilename namedentityrecognitionclass regexdeidentificationclass george");
			System.exit(0);
		}
		boolean george = false;
		if( args.length == 8) {
			george = true;
		}
		String inputfile = args[0];
		String outputfile = args[1];
		//		String whitelistfile = null;
		String blacklistfile = args[2];
		String namedentityrecognitionclass = args[3];
		String regexdeidentificationclass = args[4];
		int recordsPerThread = Integer.parseInt(args[5]);
		int nThreads = Integer.parseInt(args[6]);

		DeIdentification deidentification = new DeIdentification();
		try {
			deidentification.init(blacklistfile, namedentityrecognitionclass, regexdeidentificationclass);
		} catch (Exception e) {
			e.printStackTrace();
		}

		namedEntityRecognition.setWhiteListMap(whiteListMap);
		namedEntityRecognition.setBlackListMap(blackListMap);

		///////////////////////////////////////////////////////////
		java.util.Date startDate = new java.util.Date();

		////////////////////////////////////////////////////////
		// read database records with supplied query
		List<DeIdentificationThread> threadList = new ArrayList<DeIdentificationThread>();

		try {
			DeIdentificationThread newT = new DeIdentificationThread(namedEntityRecognition);
			boolean patNameProvided = false;

			String line = null;

			Configuration conf = new Configuration();
			FileSystem inputfs = FileSystem.get(URI.create(inputfile), conf);
			FSDataInputStream inputStream = inputfs.open(new Path(inputfile));
			FileSystem outputfs = FileSystem.get(URI.create(outputfile), conf);
			Path path = new Path(outputfile);
			if ( outputfs.exists( path )) { outputfs.delete( path, true ); } 
			OutputStream outputStream = outputfs.create( path );

			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));

			if( george ) {
				line = br.readLine(); // header
			}
			int count=0;
			while((line = br.readLine()) != null) {
				System.out.print("*");
				
				String id = "";
				String PAT_ID_ENC = "";
				String date_shift = "";
				int dateOffset = 0;
				String PATIENT_NUM = "";
				String PAT_ENC_CSN_ID = "";
				String encounter_num = "";
				String pat_first_name = "";
				String pat_last_name = "";
				String patientName = "";

				String CONTACT_DATE = "";
				String IP_NOTE_TYPE_C = "";
				String text = "";
				
				// System.out.println(line);
				String [] tokens = line.split("\t");
				if( george ) {
                   id = tokens[0].trim();
                   pat_first_name = tokens[1].trim();
                   pat_last_name = tokens[2].trim();
                   patientName = pat_first_name + " " + pat_last_name;
                   date_shift = tokens[3].trim();
                   dateOffset = Integer.parseInt(date_shift);
                   text = tokens[4].trim();
				}
				else {
					//PAT_ID|PAT_ID_ENC|DATE_OFF|PATIENT_NUM|PAT_ENC_CSN_ID|encounter_num|pat_first_name|pat_last_name|pat_middle_name|  PAT_ID|PAT_ENC_CSN_ID|CONTACT_DATE|IP_NOTE_TYPE_C|           NOTE_TEXT
					id = tokens[0].trim();
					PAT_ID_ENC = tokens[1].trim();
					date_shift = tokens[2].trim();
					dateOffset = Integer.parseInt(date_shift);
					PATIENT_NUM = tokens[3].trim();
					PAT_ENC_CSN_ID = tokens[4].trim();
					encounter_num = tokens[5].trim();
					pat_first_name = tokens[6].trim();
					pat_last_name = tokens[7].trim();
					patientName = pat_first_name + " " + pat_last_name;
					CONTACT_DATE = tokens[11].trim();
					IP_NOTE_TYPE_C = tokens[12].trim();
					text = tokens[13].trim();
				}

				java.util.Date startDateRegex = new java.util.Date();
				String preprocessedText = deidentificationRegex.compositeRegex(text, dateOffset, blackListMap, patientName);
				java.util.Date endDateRegex = new java.util.Date();
				MedicalRecordWrapper record = new MedicalRecordWrapper(id, "n1", text, preprocessedText, null, dateOffset, patientName);
				long msecs = SimpleIRUtilities.getElapsedTimeMilliseconds(startDateRegex, endDateRegex);
				record.setMillisecondsRegex(msecs);

				// for search
				record.setDate( ((deidentification.mcw.DeidentificationRegexMCW) deidentificationRegex).dateRegex(CONTACT_DATE, dateOffset) );
				record.setEncounterNumber(encounter_num);
				record.setPatientNumber(PATIENT_NUM);

				newT.getRecordList().add(record);

				if (newT.getRecordList().size() >= recordsPerThread) {
					System.out.println();
					if (threadList.size() >= nThreads) {
						DeIdentificationThread t = threadList.remove(0);
						try {
							t.join();
							for(MedicalRecordWrapper r : t.getRecordList()) {
								//                        		System.out.println(deidText);
								if( george ) {
									bw.write(r.getId() + "\t" + r.getDeIdText() + "\n");
								}
								else {
									bw.write(r.getPatientNumber() + "\t" + r.getEncounterNumber() + "\t" + r.getDate() + "\t" + r.getDeIdText() + "\n");
								}
								System.out.print(" " + ++count);
							}
							bw.flush();
							System.out.println();

						} catch (InterruptedException e) {
							e.printStackTrace();
							return;
						}
					}

					threadList.add(newT);
					newT.start();
					newT = new DeIdentificationThread(namedEntityRecognition);
				}
			}

			// process left-overs
			if (newT.getRecordList().size() > 0) {

				threadList.add(newT);
				newT.start();
			}

			// wait for processing to finish
			try {
				for (DeIdentificationThread t : threadList) {
					t.join();
					System.out.println();
					for(MedicalRecordWrapper r : t.getRecordList()) {
						if( george ) {
							bw.write(r.getId() + "\t" + r.getDeIdText() + "\n");
						}
						else {
							bw.write(r.getPatientNumber() + "\t" + r.getEncounterNumber() + "\t" + r.getDate() + "\t" + r.getDeIdText() + "\n");
						}
						System.out.print(" " + ++count);
					}
					System.out.println();
				}
				bw.flush();
				bw.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			java.util.Date endDate = new java.util.Date();
			System.out.println(SimpleIRUtilities.getElapsedTime(startDate, endDate));

		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
