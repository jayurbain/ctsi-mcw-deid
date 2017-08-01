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

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

import deidentification.DeidentificationRegex;
import deidentification.NamedEntityRecognition;

public class WordCount extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(WordCount.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new WordCount(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "wordcount");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	  
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long numRecords = 0;    
    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
    
	// deid
    final static Logger LOG = Logger.getLogger(DeIdentificationMapReduce.class);
	final static NumberFormat formatter = new DecimalFormat("#0.00000");
	NamedEntityRecognition namedEntityRecognition = null;
	DeidentificationRegex deidentificationRegex = null;
	HashMap<String, String> whiteListMap = null;
	HashMap<String, String> blackListMap = null;
	
	// debug
	String pad = "nopad";
	String namedentityrecognitionclass = "deidentification.mcw.NamedEntityRecognitionMCW";

    @Override
	protected void setup(Context context) throws IOException, InterruptedException {

    	System.out.println("in setup()");
    	Configuration conf = context.getConfiguration();
		try {
			namedEntityRecognition = (NamedEntityRecognition) Class.forName(namedentityrecognitionclass).newInstance();
			pad = "padded";
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

//		File backlistfile = new File(blacklistfilename);

		// read black list - block list
//		try {
//			blackListArray = loadFileList(backlistfile);
//		} catch (IOException e2) {
//			e2.printStackTrace();
//		}

		whiteListMap = new HashMap<String, String>();
		blackListMap = new HashMap<String, String>();

//		if (blackListArray != null && blackListArray.length > 0) {
//			for (int i = 0; i < blackListArray.length; i++) {
//				String[] stringArray = blackListArray[i].split("\\s+");
//				for (int j = 0; j < stringArray.length; j++) {
//					String s = stringArray[j].toLowerCase();
//					blackListMap.put(s, s);
//				}
//			}
//		}

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

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
      String line = lineText.toString();
      Text currentWord = new Text();
      for (String word : WORD_BOUNDARY.split(line)) {
        if (word.isEmpty()) {
            continue;
        }
            currentWord = new Text(word+pad);
            context.write(currentWord,one);
        }
    }
  }

  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }
}
