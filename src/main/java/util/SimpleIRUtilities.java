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
package util;
import java.util.Date;
import java.util.*;


/**
 * Title:        New IR System
 * Description:
 * Copyright:    Copyright (c) 2000
 * Company:      IIT
 * @author
 * @version 1.0
 */

public class SimpleIRUtilities   {

         /* return time to build or load the index */
  public static String getElapsedTime(Date startDate, Date endDate) {

    double milliseconds;
    double seconds;
    long minutes;
    long hours;
    String result;

    result = new String();
    long j = 0;
    milliseconds = endDate.getTime() - startDate.getTime();
    
    //Calendar.get(Calendar.SECOND);
    seconds = milliseconds / 1000;
    //System.out.println("endDate.getTime()="+endDate.getTime()+", startDate.getTime()="+startDate.getTime());
    result = "Seconds: " + seconds;
    /*
    hours = seconds / 3600;
    seconds = seconds - (hours * 3600);
    minutes = seconds / 60;
    seconds = seconds - (minutes * 60);
    microseconds = microseconds - (((hours * 3600) + (minutes * 60) + seconds) * 1000);
    result = hours + ":" + minutes + ":" + seconds + "." + microseconds;
    */
    return result;
  }
  
  public static long getElapsedTimeMilliseconds(Date startDate, Date endDate) {

	    long milliseconds;
	    long seconds;
	    long minutes;
	    long hours;

	    long j = 0;
	    milliseconds = endDate.getTime() - startDate.getTime();
	    
	    //Calendar.get(Calendar.SECOND);
	    seconds = milliseconds / 1000;

	    return milliseconds;
	  }
  
  public static long getElapsedTimeSeconds(Date startDate, Date endDate) {

	    long milliseconds;
	    long seconds;
	    long minutes;
	    long hours;

	    long j = 0;
	    milliseconds = endDate.getTime() - startDate.getTime();
	    
	    //Calendar.get(Calendar.SECOND);
	    seconds = milliseconds / 1000;

	    return seconds;
	  }

  /* pad document id with blanks if necessary     */
  /* this is just for debugging purposes          */
  public static String longToString (long l) {
    if (l  > 999999999 ) return ""+l;
    if (l  > 99999999 )  return " "+l;
    if (l  > 9999999 )   return "  "+l;
    if (l  > 999999 )    return "   "+l;
    if (l  > 99999 )     return "    "+l;
    if (l  > 9999 )      return "     "+l;
    if (l  > 999 )       return "      "+l;
    if (l  > 99 )        return "       "+l;
    if (l  > 9 )         return "        "+l;
    return "         "+l;
  }

  /* pad token with blanks if necessary, just for debugging output  */
  public static String padToken(String token) {

    int MAX_TOKEN_SIZE = 30;
    StringBuffer s = new StringBuffer(token);

    /* pad token with blanks if necessary */
    int len = token.length();
    if (len < MAX_TOKEN_SIZE) {
        for (int j=token.length(); j < MAX_TOKEN_SIZE; j++) {
             s.append(" ");
        }
    } else {
        /* do a set length to truncate long tokens */
        s.setLength(MAX_TOKEN_SIZE);
    }
    return s.toString();
  }
  
  /**
  * Jay Urbain - 9/17/03
  *
  * Expects object of type Integer as value
  */
  
public static Map.Entry[] sortHashMap(HashMap map, boolean sortKey) {

  Object[] entry = map.entrySet().toArray();
  Map.Entry[] result = new Map.Entry[entry.length];
  if (entry.length > 0) {

    System.arraycopy(entry, 0, result, 0, entry.length);
    if( !sortKey ) {
      Arrays.sort(result, new Comparator () {
        public int compare (Object o1, Object o2) {
          Map.Entry a = (Map.Entry)o1, b = (Map.Entry)o2;
          int cmp = ((Integer) a.getValue()).compareTo((Integer)b.getValue());
          return cmp != 0 ? cmp : ((String)a.getKey()).compareTo((String)b.getKey());
        }
      });
    }
    else { // just sort on key
      Arrays.sort(result, new Comparator () {
        public int compare (Object o1, Object o2) {
          Map.Entry a = (Map.Entry)o1, b = (Map.Entry)o2;
          return ((String)a.getKey()).compareTo((String)b.getKey());
        }
      });
    }

  }
  return result;
}

}
