// Copyright (c) 2010 Jay F. Urbain. All Rights Reserved.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
//
// For more information, bug reports, fixes, contact:
//    Jay Urbain
//    jay.urbain@gmail.com

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
