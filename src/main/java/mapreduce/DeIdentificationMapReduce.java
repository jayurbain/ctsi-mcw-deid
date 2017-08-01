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
package mapreduce;

// java
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
import java.util.*;
import java.util.regex.Pattern;

// hadoop
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

// deid
import deidentification.DeidentificationRegex;
import deidentification.MedicalRecordWrapper;
import deidentification.NamedEntityRecognition;
import util.SimpleIRUtilities;

/**
 * @author jayurbain
 */
public class DeIdentificationMapReduce extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out.println("main: " +  args);
		int res = ToolRunner.run(new DeIdentificationMapReduce(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		System.out.println("run: " +  args);
		if( args.length < 5) {
			System.out.println("args: inputfile outputfile blacklistfilename namedentityrecognitionclass regexdeidentificationclass");
			System.exit(-1);
		}
		
		String blacklistfile = args[2];
		String namedentityrecognitionclass = args[3];
		String regexdeidentificationclass = args[4];

		Configuration conf = getConf();
		conf.set("blacklistfile", blacklistfile);
		conf.set("namedentityrecognitionclass", namedentityrecognitionclass);
		conf.set("regexdeidentificationclass", regexdeidentificationclass);
		
		Job job = Job.getInstance(getConf(), "DeIdentificationMapReduce");
		job.setJarByClass(this.getClass());

		// Use TextInputFormat, the default unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		final static Logger LOG = Logger.getLogger(DeIdentificationMapReduce.class);
		final static NumberFormat formatter = new DecimalFormat("#0.00000");
		NamedEntityRecognition namedEntityRecognition = null;
		DeidentificationRegex deidentificationRegex = null;
		HashMap<String, String> whiteListMap = null;
		HashMap<String, String> blackListMap = null;

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private long numRecords = 0;    
		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
		
        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
         * load stop words file into stopWords set
         */
        @Override
		protected void setup(Context context) throws IOException, InterruptedException {

        	System.out.println("in setup()");
        	Configuration conf = context.getConfiguration();
        	System.out.println("" + conf.get("blackfilelist") + conf.get("namedentityrecognition") + conf.get("deidentificationregex"));
			try {
				init(conf.get("blackfilelist"), conf.get("namedentityrecognition"), conf.get("deidentificationregex"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("exit setup()");
		}
        
    	public void init(String blacklistfilename, String namedentityrecognitionclass, String regexdeidentificationclass)  throws Exception {

    		System.out.println("in init()");
    		System.out.println(blacklistfilename + ", " + namedentityrecognitionclass + ", " + regexdeidentificationclass);
    		String[] whiteListArray = null;
    		String[] blackListArray = null;

//    		File backlistfile = new File(blacklistfilename);

    		// read black list - block list
//    		try {
//    			blackListArray = loadFileList(backlistfile);
//    		} catch (IOException e2) {
//    			e2.printStackTrace();
//    		}

    		whiteListMap = new HashMap<String, String>();
    		blackListMap = new HashMap<String, String>();

//    		if (blackListArray != null && blackListArray.length > 0) {
//    			for (int i = 0; i < blackListArray.length; i++) {
//    				String[] stringArray = blackListArray[i].split("\\s+");
//    				for (int j = 0; j < stringArray.length; j++) {
//    					String s = stringArray[j].toLowerCase();
//    					blackListMap.put(s, s);
//    				}
//    			}
//    		}

    		try {
    			namedEntityRecognition = (NamedEntityRecognition) Class.forName(namedentityrecognitionclass).newInstance();
    			System.out.println("CLASS TO DEID WITH : " + namedentityrecognitionclass);
    			deidentificationRegex = (DeidentificationRegex) Class.forName(regexdeidentificationclass).newInstance();
    		} catch (Exception e1) {
    			new Exception(e1);
    		}
    		namedEntityRecognition.setWhiteListMap(whiteListMap);
    		namedEntityRecognition.setBlackListMap(blackListMap);
    		
    		System.out.println("exit init()");
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

		public void map(LongWritable offset, Text lineText, Context context)
				throws IOException, InterruptedException {
			
			System.out.println("in map()");
			
			String line = lineText.toString();
			StringBuffer sbb = new StringBuffer();
			String [] tokens = line.split("\t");

			//PAT_ID|PAT_ID_ENC|DATE_OFF|PATIENT_NUM|PAT_ENC_CSN_ID|encounter_num|pat_first_name|pat_last_name|pat_middle_name|  PAT_ID|PAT_ENC_CSN_ID|CONTACT_DATE|IP_NOTE_TYPE_C|           NOTE_TEXT
			String id = tokens[0].trim();
			String PAT_ID_ENC = tokens[1].trim();
			String date_shift = tokens[2].trim();
			int dateOffset = Integer.parseInt(date_shift);
			String PATIENT_NUM = tokens[3].trim();
			String PAT_ENC_CSN_ID = tokens[4].trim();
			String encounter_num = tokens[5].trim();
			String pat_first_name = tokens[6].trim();
			String pat_last_name = tokens[7].trim();
			String patientName = pat_first_name + " " + pat_last_name;
			String CONTACT_DATE = tokens[11].trim();
			String IP_NOTE_TYPE_C = tokens[12].trim();
			String text = tokens[13].trim();

//			String preprocessedText = deidentificationRegex.compositeRegex(text, dateOffset, blackListMap, patientName);
			String preprocessedText = text;

			MedicalRecordWrapper record = new MedicalRecordWrapper(id, "n1", text, preprocessedText, null, dateOffset, patientName);
			// for search
//			record.setDate( ((deidentification.mcw.DeidentificationRegexMCW) deidentificationRegex).dateRegex(CONTACT_DATE, dateOffset) );
			record.setDate( CONTACT_DATE );
			
			record.setEncounterNumber(encounter_num);
			record.setPatientNumber(PATIENT_NUM);
//			record.setDeIdText( namedEntityRecognition.performAnnotation( record.getRegexText() ) );
			record.setDeIdText( text );
			String rr = record.getPatientNumber() + "\t" + record.getEncounterNumber() + "\t" + record.getDate() + "\t" + record.getDeIdText() + "\n";
			context.write(new Text("deid"), new Text(record.getDeIdText()) );
			System.out.println("exit map()");
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text id, Text text, Context context)
				throws IOException, InterruptedException {
			context.write(id, text);
		}
	}
}
