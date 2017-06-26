/**
 * 
 */
package deidentification.mcw.generation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author jayurbain
 *
 */
public class DeidentificationRegexMCW implements deidentification.DeidentificationRegex{

	/**
	 * @param String - text to convert
	 */
	private static final String dateRegex1 = "\\b([0-9]{1,2})[/\\-]([0-9]{4})\\b"; // month/year 
//	private static final String dateRegex2 = "(\\s+|^)([0-9]{1,2})[/\\-]([0-9]{1,2})"; // month/day
//	private static final String dateRegex2 = "([0-9]{1,2})[/\\-]([0-9]{1,2})"; // month/day
	private static final String dateRegex2 = "\\b([0-9]{1,2})[/\\-]([0-9]{1,2})\\b"; // month/day
	private static final String dateRegex3 = "\\b([0-9]{1,2})[/]([0-9]{1,2})[/]\\s*([0-9]{1,4})\\b"; // month/day/year - run before month/day
	private static final String dateRegex33= "\\b([0-9]{1,2})[\\-]([0-9]{1,2})[\\-]\\s*([0-9]{1,4})\\b"; // month/day/year - run before month/day
//	private static final String dateRegex4 = "([ADFJMNOS]\\w*)\\s+([0-9]{1,2})(th|TH|nd|ND){0,1},{0,1}\\s+([0-9]{4})";
	private static final String dateRegex4 = "\\b([ADFJMNOS]\\w*)\\s+([0-9]{0,2})(th|TH|nd|ND){0,1},{0,1}\\s+([0-9]{4})\\b";
	private static final String dateRegex5 = "\\b([ADFJMNOS]\\w*)\\s+([0-9]{4})\\b";
	private static final String dateRegex6 = "\\b([ADFJMNOS]\\w*)\\s+([0-9]{0,2})(th|TH|nd|ND){0,1},{0,1}\\b";
//	private static final String phoneNumberRegex = "\\({0,1}([0-9]{3})\\){0,1}[\\-|\\s|\\.]{0,1}([0-9]{3})[\\-|\\s|\\.]{0,1}([0-9]{4})";
	private static final String phoneNumberRegex = "\\({0,1}([0-9]{3})\\){0,1}[\\-|\\s|\\.]{0,1}([0-9]{3})[\\-|\\.]{0,1}([0-9]{4})\\b";
//	private static final String phoneNumberRegex2 = "([0-9]{3})[\\-|\\s|\\.]{0,1}([0-9]{4})";
	private static final String phoneNumberRegex2 = "\\b([0-9]{3})[\\-|\\.]{0,1}([0-9]{4})\\b";
//	private static final String phoneNumberRegexOfficial = "^(?:(?:\\+?1\\s*(?:[.-]\\s*)?)?(?:\\(\\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\\s*\\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\\s*(?:[.-]\\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\\s*(?:[.-]\\s*)?([0-9]{4})(?:\\s*(?:#|x\\.?|ext\\.?|extension)\\s*(\\d+))?$";

	// [-0-9a-zA-Z.+_]+@[-0-9a-zA-Z.+_]+\.[a-zA-Z]{2,4}
	private static final String emailRegex = "[-0-9a-zA-Z.+_]+@[-0-9a-zA-Z.+_]+\\.[a-zA-Z]{2,4}";
	
	// ^\d{5}(?:[-\s]\d{4})?$
//	private static final String zipRegex = "(\\s+|^)\\d{5}(?:[-\\s]\\d{4})?$";
	private static final String zipRegex = "\\b\\d{5}((-)\\d{4})?\\b";
	
//	private static final String addressRegex = "(^|\\s+)(\\d+)(\\s+)((?i)[NSEW](?i-))\\.{0,1}(\\w+)(\\s+|$)";
//	private static final String addressRegex = "(^|\\s+)(\\d+)(\\s+)((?i)[NSEW]{1}(?i-))\\.{0,1}\\s+(\\w+)";
	private static final String addressRegex = "(\\d+)(\\s+)((?i)[NSEW]{1}(?i-))\\.{0,1}\\s+(\\w+)";
	
	private static Pattern datePattern1 = Pattern.compile(dateRegex1);
	private static Pattern datePattern2 = Pattern.compile(dateRegex2);
	private static Pattern datePattern3 = Pattern.compile(dateRegex33);
	private static Pattern datePattern33 = Pattern.compile(dateRegex3);
	private static Pattern datePattern4 = Pattern.compile(dateRegex4);
	private static Pattern datePattern5 = Pattern.compile(dateRegex5);
	private static Pattern datePattern6 = Pattern.compile(dateRegex6);
	private static Pattern phoneNumberPattern = Pattern.compile(phoneNumberRegex);
	private static Pattern phoneNumberPattern2 = Pattern.compile(phoneNumberRegex2);
	
	private static Pattern emailPattern = Pattern.compile(emailRegex);
	
	private static Pattern zipPattern = Pattern.compile(zipRegex);
	
	private static Pattern addressPattern = Pattern.compile(addressRegex);
	
//	private final static String mixedCaseAlphaNumericRegex1 = "([a-z|A-Z])([0-9])"; // would separate A1C
//	private final static String mixedCaseAlphaNumericRegex2 = "([0-9])([a-z|A-Z])"; v// would separate 3rd
	private final static String mixedCaseAlphaNumericRegex2 = "(\\s+|^)([0-9]+)([A-Z])";
	private final static String mixedCaseAlphaNumericRegex3 = "([a-z])([A-Z])";
	private final static String [] monthArrayLong = {"january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"};
	private final static String [] monthArrayShort = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "sept"};
	
	
	public static String mixedCaseAlphaNumericTransition(String s) {
//		return s.replaceAll(mixedCaseAlphaNumericRegex1,replacement).replaceAll(mixedCaseAlphaNumericRegex2,replacement).replaceAll(mixedCaseAlphaNumericRegex3,replacement);
		return s.replaceAll(mixedCaseAlphaNumericRegex2," $2 $3").replaceAll(mixedCaseAlphaNumericRegex3,"$1 $2");
	}
	
	/**
	 * @param String - text to convert
	 */
	public static String phoneNumberRegex(String s) {

		Matcher matcher = phoneNumberPattern.matcher(s);
		StringBuffer bufStr = new StringBuffer();
		while( matcher.find() ) {
//			matcher.appendReplacement(bufStr, " [xxx_xxx_xxxx] ");
			matcher.appendReplacement(bufStr, randomPhoneNumberRegex());
		}
		matcher.appendTail(bufStr);
		
		String s2 = bufStr.toString();
		String s3 = phoneNumberRegex2(s2);
		
		return s3;
	}
	
	public static String randomPhoneNumberRegex() {
		String s = "" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 );
		return s;
	}
	
	/**
	 * @param String - text to convert
	 */
	public static String phoneNumberRegex2(String s) {

		Matcher matcher = phoneNumberPattern2.matcher(s);
		StringBuffer bufStr = new StringBuffer();
		while( matcher.find() ) {
//			matcher.appendReplacement(bufStr, " [xxx_xxxx] ");
			matcher.appendReplacement(bufStr, randomPhoneNumberRegex2());
		}
		matcher.appendTail(bufStr);
		return bufStr.toString();
	}
	
	public static String randomPhoneNumberRegex2() {
		String s = "" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 );
		return s;
	}
	
	/**
	 * @param String - text to convert
	 */
	public static String emailRegex(String s) {

		Matcher matcher = emailPattern.matcher(s);
		StringBuffer bufStr = new StringBuffer();
		while( matcher.find() ) {
//			matcher.appendReplacement(bufStr, " [xxx@xxx.xxx] ");
			matcher.appendReplacement(bufStr, "jay.urbain@gmail.com");
		}
		matcher.appendTail(bufStr);
		return bufStr.toString();
	}
	
	/**
	 * @param String - text to convert
	 */
	public static String zipRegex(String s) {

		Matcher matcher = zipPattern.matcher(s);
		StringBuffer bufStr = new StringBuffer();
		while( matcher.find() ) {
//			matcher.appendReplacement(bufStr, " [xxxxx] ");
			matcher.appendReplacement(bufStr, "90210");
		}
		matcher.appendTail(bufStr);
		return bufStr.toString();
	}
	
	/**
	 * @param String - text to convert
	 */
	public static String addressRegex(String s) {

		Matcher matcher = addressPattern.matcher(s);
		StringBuffer bufStr = new StringBuffer();
		while( matcher.find() ) {
//			matcher.appendReplacement(bufStr, " [xxxxx x. xxxxx] ");
			matcher.appendReplacement(bufStr, "1025 N. Broadway");
		}
		matcher.appendTail(bufStr);
		return bufStr.toString();
	}

	/**
	 * @param String - text to convert
	 */
	public static String dateRegex(String s, int dayOffset) {

//		System.out.println("Before date: " + s );
		Calendar cal = null;
		StringBuffer bufStr = new StringBuffer();
		
		Matcher matcher3 = datePattern3.matcher(s);
		bufStr = new StringBuffer();
		while( matcher3.find() ) {
			//		 	 System.out.println( matcher3.group());
			int month = Integer.parseInt(matcher3.group(1));
			int day = Integer.parseInt(matcher3.group(2));
			int year = Integer.parseInt(matcher3.group(3));
			String zeropad="";
			if( year < 10) {
				zeropad="0";
			}
			cal =  createConvertedDate(month, day, year, dayOffset);
//			String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + zeropad + (cal.get(Calendar.YEAR)) + "] ";
			String newDateString = "" + (cal.get(Calendar.MONTH)+1) + "/" + (cal.get(Calendar.DAY_OF_MONTH)) + "/" + zeropad + (cal.get(Calendar.YEAR)) + "";//			

			if( month > 12) { // not a date
//				newDateString = " [" + matcher3.group(1) + "/" + matcher3.group(2) + "/" + matcher3.group(3) + "] ";
				newDateString = "" + matcher3.group(1) + "/" + matcher3.group(2) + "/" + matcher3.group(3) + "";
			}
			matcher3.appendReplacement(bufStr, newDateString);
		}
		matcher3.appendTail(bufStr);
		
		Matcher matcher33 = datePattern33.matcher(bufStr.toString());
		bufStr = new StringBuffer();
		while( matcher33.find() ) {
			//		 	 System.out.println( matcher33.group());
			int month = Integer.parseInt(matcher33.group(1));
			int day = Integer.parseInt(matcher33.group(2));
			int year = Integer.parseInt(matcher33.group(3));
			String zeropad="";
			if( year < 10) {
				zeropad="0";
			}
			cal =  createConvertedDate(month, day, year, dayOffset);
//			String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + zeropad + (cal.get(Calendar.YEAR)) + "] ";
			String newDateString = "" + (cal.get(Calendar.MONTH)+1) + "/" + (cal.get(Calendar.DAY_OF_MONTH)) + "/" + zeropad + (cal.get(Calendar.YEAR)) + "";
			if( month > 12) { // not a date
//				newDateString = " [" + matcher33.group(1) + "/" + matcher33.group(2) + "/" + matcher33.group(3) + "] ";
				newDateString = "" + matcher33.group(1) + "/" + matcher33.group(2) + "/" + matcher33.group(3) + "";
			}
			matcher33.appendReplacement(bufStr, newDateString);
		}
		matcher33.appendTail(bufStr);
		
		
		Matcher matcher4 = datePattern4.matcher(bufStr.toString());
		bufStr = new StringBuffer();
		while( matcher4.find() ) {
			//		 	 System.out.println( matcher3.group());
			try {
				int month = convertDateNameToNumber(matcher4.group(1)); // String month
				if( month <= 0 ) {
					continue;
				}
				String dayStr = matcher4.group(2);
				if( !(dayStr != null && dayStr.length() > 0 ) ) {
					continue;
				}
				int day = Integer.parseInt(dayStr);
				int year = Integer.parseInt(matcher4.group(4));
				String zeropad="";
				if( year < 10) {
					zeropad="0";
				}
				cal =  createConvertedDate(month, day, year, dayOffset);
//				String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + zeropad + (cal.get(Calendar.YEAR)) + "] ";
				String newDateString = "" + (cal.get(Calendar.MONTH)+1) + "/" + (cal.get(Calendar.DAY_OF_MONTH)) + "/" + zeropad + (cal.get(Calendar.YEAR)) + "";

				matcher4.appendReplacement(bufStr, newDateString);
			} catch (NumberFormatException e) {
//				System.out.println("matcher4.group(1):"+matcher4.group(1) + "; matcher4.group(2):" + matcher4.group(2) + "; matcher4.group(4): "+ matcher4.group(4));
				e.printStackTrace();
			}
		}
		matcher4.appendTail(bufStr);
		
		Matcher matcher5 = datePattern5.matcher(bufStr.toString());
		bufStr = new StringBuffer();
		while( matcher5.find() ) {
			//		 	 System.out.println( matcher3.group());
			int month = convertDateNameToNumber(matcher5.group(1)); // String month
			int day = 15;
			int year = Integer.parseInt(matcher5.group(2));
			String zeropad="";
			if( year < 10) {
				zeropad="0";
			}
			cal =  createConvertedDate(month, day, year, dayOffset);
//			String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + zeropad + (cal.get(Calendar.YEAR)) + "] ";
			String newDateString = "" + (cal.get(Calendar.MONTH)+1) + "/" + (cal.get(Calendar.DAY_OF_MONTH)) + "/" + zeropad + (cal.get(Calendar.YEAR)) + "";

			matcher5.appendReplacement(bufStr, newDateString);
		}
		matcher5.appendTail(bufStr);
		
		Matcher matcher6 = datePattern6.matcher(bufStr.toString());
		bufStr = new StringBuffer();
		while( matcher6.find() ) {
			//		 	 System.out.println( matcher3.group());
			int month = convertDateNameToNumber(matcher6.group(1)); // String month
			if( month <= 0 ) {
				continue;
			}
			String dayString = matcher6.group(2);
			int day = 0;
			try {
				day = Integer.parseInt(matcher6.group(2));
			} catch (NumberFormatException e) {
				continue;
			}
			int year = 0;
			String zeropad="";
			cal =  createConvertedDate(month, day, year, dayOffset);
//			String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + zeropad + (cal.get(Calendar.YEAR)) + "] ";
			String newDateString = "" + (cal.get(Calendar.MONTH)+1) + "/" + (cal.get(Calendar.DAY_OF_MONTH)) + "/" + zeropad + (cal.get(Calendar.YEAR)) + "";

			matcher6.appendReplacement(bufStr, newDateString);
		}
		matcher6.appendTail(bufStr);
		
//		private static final String dateRegex1 = "([0-9]{1,2})[/\\-]([0-9]{4})"; // month/year 
		Matcher matcher1 = datePattern1.matcher(bufStr.toString());
		bufStr = new StringBuffer();
		while( matcher1.find() ) {
			//		 	 System.out.println( matcher1.group());
			int month = Integer.parseInt(matcher1.group(1));
			int day = 15; // arbitrary date see for obfuscation
			int year = Integer.parseInt(matcher1.group(2));
			// restrict conversion
			if( month>= 1 && month <= 12 && year > 1900 && year < 3000)  {
				cal =  createConvertedDate(month, day, year, dayOffset);
//				String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + (cal.get(Calendar.YEAR)) + "] ";
				String newDateString = "" + (cal.get(Calendar.MONTH)+1) + "/" + (cal.get(Calendar.DAY_OF_MONTH)) + "/" + (cal.get(Calendar.YEAR)) + "";

				matcher1.appendReplacement(bufStr, newDateString);
			}
		}
		matcher1.appendTail(bufStr);

//		private static final String dateRegex2 = "([0-9]{1,2})[/]([0-9]{1,2})"; // month/day or month year
//		Matcher matcher2 = datePattern2.matcher(bufStr.toString());
//		bufStr = new StringBuffer();
//		while( matcher2.find() ) {
//			//		 	 System.out.println( matcher2.group());
//			int month = Integer.parseInt(matcher2.group(1));
//			int day = Integer.parseInt(matcher2.group(2));
//			int year = 0;
//			// restrict conversion
//			if( month>= 1 && month <= 12 && day > 0 && day < 99)  { // Note: day could be year
//				cal =  createConvertedDate(month, day, year, dayOffset);
//				String newDateString = " [" + (cal.get(Calendar.MONTH)+1) + "_" + (cal.get(Calendar.DAY_OF_MONTH)) + "_" + (cal.get(Calendar.YEAR)) + "] ";
//				matcher2.appendReplacement(bufStr, newDateString);
//			}
//		}
//		matcher2.appendTail(bufStr);

		//		String s2 = s.replaceAll(dateRegex3, "$1|$2|$3").replaceAll(dateRegex2, "$1|$2");
//		System.out.println("After date: " + bufStr.toString() );
		return bufStr.toString();
	}
	
	/**
	 * Adjust given month, day, and optional year to date with dayOffset
	 * dayOffset could be positive or negative
	 * 0 for year means no year available, so use current year
	 * 
	 *  @param String - text to convert
	 */
	public static Calendar createConvertedDate(int month, int day, int year, int dayOffset) {

		Calendar cal = new GregorianCalendar();
		cal.set(Calendar.MONTH, month-1);
		cal.set(Calendar.DAY_OF_MONTH, day);
		if( year > 0 ) {
			cal.set(Calendar.YEAR, year);
		}

		// Get the number of days in that month
		int daysInMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		if( day + dayOffset > daysInMonth ) { 
			month = month+1;
			cal.set(Calendar.MONTH, (month-1)); // Java counts months starting at 0
			cal.set(Calendar.DAY_OF_MONTH, day + dayOffset - daysInMonth);
		}
		else if( day + dayOffset < 1 ) {
			month = month-1;
			cal.set(Calendar.MONTH, (month-1));
			daysInMonth = cal.getActualMaximum(Calendar.DAY_OF_MONTH); // update days in month
			cal.set(Calendar.DAY_OF_MONTH, daysInMonth + dayOffset + day);
		}
		else {
			cal.set(Calendar.DAY_OF_MONTH, day+dayOffset);
		}
		
		// Get the number of months in year
		if( month > 12 ) { 
			cal.set(Calendar.YEAR, year+1);
			cal.set(Calendar.MONTH, (1) -1);
		}
		else if( month < 1 ) {
			cal.set(Calendar.YEAR, year-1);
			cal.set(Calendar.MONTH, (12) -1);
		}
		
		return cal;
	}
	
	public static int convertDateNameToNumber(String s) {
		
		int i=0;
		String ss = s.toLowerCase();
		for( i=0; i< monthArrayLong.length; i++) {
			if( ss.equals(monthArrayLong[i])) {
				return i+1;
			}
		}
		for( i=0; i< monthArrayShort.length; i++) {
			if( ss.equals(monthArrayShort[i])) {
				return i+1;
			}
		}
		return 0;
	}

	/**
	 * De-identify MRN here, as ID entities was turned off in MIST since 
	 * their algorithm considers some procedures words like Spirometry and Pap as identifiers.
	 * 
	 * Note: An MRN can come as 01234567 or even without leading zeros. It can also
	 * appear as 01-23-45-67.
	 * 
	 * @param String - text to convert
	 */
//	private static final String patientidRegex1 = "([0-9]{7,8})";
//	private static final String patientidRegex1 = "\\b([0-9]{6,9})\\b";
	private static final String patientidRegex1 = "([0-9]{6,9})";
	private static final String patientidRegex2 = "([0-9]{1,2})\\-([0-9]{1,2})\\-([0-9]{1,2})\\-([0-9]{1,2})\\-([0-9]{1,2})";

	public static String mrnRegex(String s) {

//		String s1 = s.replaceAll(patientidRegex1, " [xxxxxxxx] ");
//		String s2 = s1.replaceAll(patientidRegex2, " [xx-xx-xx-xx-xx] ");
		String s1 = s.replaceAll(patientidRegex1, randomMRN8());
		String s2 = s1.replaceAll(patientidRegex2, randomMRN10());
		return s2;
	}
	
	public static String randomMRN8() {
		String s = "" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 );
		return s;
	}
	
	public static String randomMRN10() {
		String s = "" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 ) +
				"-" +
				(int) Math.round( Math.random()*9 ) +
				(int) Math.round( Math.random()*9 );
		return s;
	}

	/**
	 * Convert selected procedure names to ALL CAPS prior to running
	 * through MIST deidentification as the following examples shows
	 * that MIST identifies certain procedures as identifiers or names. 
	 * If we first convert to ALL CAPS it will not try to deid.
	 * Example:
	 * 126	10534	Spirometry 6/10/04  	Zeeevudjpc 9/2314
	 * 
	 * @param String - text to convert
	 */
	private static String [] procedureAllCapsArray = {"COLONOSCOPY", "PAP"};
	public static String procedureAllCaps(String s) {

		for(int i=0; i<procedureAllCapsArray.length; i++) {
			s = s.replaceAll("(?i)"+procedureAllCapsArray[i], procedureAllCapsArray[i] );
		}
		return s;
	}
	
	public static String whiteList(String s, String [] whiteListArray) {

		for(int i=0; i<whiteListArray.length; i++) {
			s = s.replaceAll("(?i)"+whiteListArray[i], whiteListArray[i] );
		}
		return s;
	}
	
	public static boolean inWordList(String s, String [] listArray) {

		for(int i=0; i<listArray.length; i++) {
			if( s.toLowerCase().equals( listArray[i] ) ) {
				return true;
			}
		}
		return false;
	}
	
	public static String blackList(String s, Map<String, String> blackListMap) {

		Set<String> set = blackListMap.keySet();
		for(String key : set) {
//			s = s.replaceAll("(?i)(\\s+|^)"+key, " [XXXXX] " );	
			s = s.replaceAll("(?i)"+key, " [BLACK] " );	
		}
		return s;
	}
	
	static String [] personFirst = 
		{"Sergey",
		"Alan",
		"John",
		"Donal",
		"Grace",
		"Ada",
		"John",
	    "John",
	    "Larry",
	    "Edsger",
	    "James",
	    "Jim",
	    "Claude",
	    "Anita",
	    "Edger"};
	
	static String [] personLast = 
		{"Brin",
		"Turing",
		"Von Neumann",
		"Knuth",
		"Hopper",
		"Lovelace",
		"Backus",
	    "McCarthy",
	    "Page",
	    "Dijkstra",
	    "Gosling",
	    "Gray",
	    "Shannon",
	    "Borg",
	    "Codd"};
	
	/**
	 * Takes a String s (patient record), and a String for the patientName 
		(Urbain, Jay F; Jay Urbain; Jay F Urbain). Replaces words in name with "[PATIENT]".
		Note: Care had to be taken when writting this method to ensure words in name
		are on word boundaries, and the middle initial, if provided, is within the position
		range of the name. Otherwise it was capturing single letter abbreviations used
		in records.
	 * @param s
	 * @param patientName
	 * @return
	 */
	
	public static String patientNameList(String s, String patientName) {

		if( patientName == null || patientName.length() == 0 ) {
			return patientName;
		}
		int maxPos = Integer.MIN_VALUE;
		int minPos = Integer.MAX_VALUE;
		
		String [] patientNameArray = patientName.split("[\\s|\\.|,]+");
		List<String> patientNameList = new ArrayList<String>(Arrays.asList(patientNameArray));
			
		// find all occurrences forward of name words > 1 character in length
		// save max, min positions for name range to limit single character middle initial replacements 
		for(String name : patientNameList) {
			StringBuffer bufStr = new StringBuffer();
			if( name.length() > 1 || patientNameList.size() > 2 ) {
				String regex = "\\b(?i)"+name+"\\b";
			    Pattern pattern = null;
			    Matcher matcher = null;
				try {
					pattern = Pattern.compile(regex);
				    matcher = pattern.matcher(s);
				    while( matcher.find() ) {
					    if( matcher.start() > maxPos) {
					    	maxPos = matcher.start();
					    }
					    if( matcher.start() < minPos) {
					    	minPos = matcher.start();
					    }
					    matcher.appendReplacement(bufStr, 
					    		deidentification.mcw.generation.NamedEntityRecognitionMCW.randomPerson(name, "PERSON"));
				    }
				    matcher.appendTail(bufStr);
				} catch (Exception e) {
					System.out.println("regex: " + regex);
					e.printStackTrace();
				}
			}
			s = bufStr.toString();
		}
		
		// second pass to take char of middle initials iff with full name range
		for(String name : patientNameList) {
			StringBuffer bufStr = new StringBuffer();
			if( name.length() == 1 || patientNameList.size() > 2 ) {
				String regex = "\\b(?i)"+name+"\\b";
			    Pattern pattern = null;
			    Matcher matcher = null;
				try {
				    pattern = Pattern.compile(regex);
				    matcher = pattern.matcher(s);
				    while( matcher.find() ) {
					    if( matcher.start() > minPos && matcher.end() < maxPos) {
					    	matcher.appendReplacement(bufStr, " [PERSON] ");
					    	break; // "should" only be one
					    }
				    }
				    matcher.appendTail(bufStr);
				} catch (Exception e) {
					System.out.println("regex: " + regex);
					e.printStackTrace();
				}
			}
			s = bufStr.toString();
		}
		return s;
	}
	
	
	/**
	 * @param String - s, text to convert
	 * @param String - dateStr, date of service
	 * @param String - dateOffset, day offset assigned to this patient for date conversion
	 * 
	 * Place preprocessing regular expression conversions method calls here
	 * Called from deidentification process.
	 * 
	 * Note: Order dependent!
	 * 
	 */

	public String compositeRegex(String s, int dateOffset, 
			Map<String, String> blackListMap, String patientName) {

//		System.out.println(s);
		s = mrnRegex(s);
		
		s = phoneNumberRegex(s);
		s = dateRegex(s, dateOffset);
		s = addressRegex(s);
		s = emailRegex(s);
		s = zipRegex(s);
		s = replaceInvalidCharacterEncoding(s);
		s = mixedCaseAlphaNumericTransition(s);
		s = patientNameList(s, patientName);
		s = s.replaceAll("\"", " ");
		s = procedureAllCaps(s);
//		System.out.println(s);

		return s;
	}
	
	/**
	 * @param String - s, text to check for invalid characters
	 * @param String - dateStr, date of service
	 * @param String - dateOffset, day offset assigned to this patient for date conversion
	 * 
	 */
	public static String replaceInvalidCharacterEncoding(String s) {
		
//		System.out.println(s); 
		String sout = s.replaceAll( "([\\ud800-\\udbff\\udc00-\\udfff])", " ");
//		System.out.println(sout); 
		return sout;
	}
	
	public static void main(String[] args) {
		
//		The following are covered:
//			return to work on 7/1/13
//			The patient will follow up in 7-23-13
//
//			I did not cover these, but will:
//			return to work on July 9th 2013
//			return to work on July 18, 2013
//			He may return to work on 7-4, 2013.
		
		DeidentificationRegexMCW deidentificationRegex = new DeidentificationRegexMCW();
		
		String [] whiteList = new String[] {"pap"};
		Map<String, String> blackListMap = new HashMap<String, String>();
//
//
		String s = "1/1CamelCase03/23PulmonarY01/9/2014Colonoscopy1/27/14 01-23-34-56-78 01234567 8/2012 2/2009";
		
		s = "119 N. Tennyson. 1414 s. Mequon Rd. 12 n 5th Street";
		String s00 = deidentificationRegex.addressRegex(s);
		System.out.println(s + " : " + s00);
		
		
		s = "Colonoscopy 2006Alpha whatever alhaBeta";
		String s0 = deidentificationRegex.mixedCaseAlphaNumericTransition(s);
		System.out.println(s + " : " + s0);
		
		s = "declined childbirth classes. 4/1: FAS3/21:  Breastfeeding. Reviewed";
		s0 = deidentificationRegex.dateRegex(s, -1);
		System.out.println(s + " : " + s0);
		
		s = "Mammo: 6-3-13.  5/4/2011 bi mammo (-). 6-5-12. Negative. Had several magnified views";
		String s2 = dateRegex(s, -10);
		System.out.println(s + " : " + s2);
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		
		//s = "2/2009";
		s = "12/27/12 12-27-12 7-4, 2013 09/7 10/2014 10/12 9/7 9/2014";
		s = "Program Coordinator901  N. 9 th Street = Courthouse Rm.   307 A [XXXXX] ,  WI 53233-1967(P)   ";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "July 9th 2013 July 9th, 2013 July 9 2013 July 18, 2013";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s2);
		s = "July 9th 2013";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "July 9th, 2013";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "July 9 2013";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "July 18, 2013";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "July 18, 2013";
		s2 = deidentificationRegex.compositeRegex(s, 10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "November 18, 2013";
		s2 = deidentificationRegex.compositeRegex(s, 10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		/////////
		s = "10/2013";
		s2 = deidentificationRegex.compositeRegex(s, 10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "December 2011";
		s2 = deidentificationRegex.compositeRegex(s, 10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "6/22/13";
		s2 = deidentificationRegex.compositeRegex(s, -10, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "10/22/13";
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "jay.urbain@gmail.com";
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		s = "urbain@msoe.edu";
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		
		s = "53217";
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		
		s = "53217-1967";
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		
		s = "53217 1967";
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		
		s = "Jay Urbain's phone number is 414-745-5102, or 745-5102, or (414) 745-5102";
		s2 = deidentificationRegex.phoneNumberRegex(s);
		System.out.println(s + " : " + s2);
		
		s = "Jay Urbain's email  is jay.urbain@gmail.com or urbain@msoe.edu";
		s2 = deidentificationRegex.emailRegex(s);
		System.out.println(s + " : " + s2);		
		
		s = "CULT ROUTINE                       COLLECTED: 11/10/04  1445ACCESSION #:  RC-04-41116                STARTED:   11/10/04  1649 \n" + 
				"SOURCE: WOUND \n" + 
				"ANUS   WOUND \n" + 
				"11/10/04  1900 \n" + 
				"----------GRAM STAIN REPORT---------- \n" + 
				"NO PMN'S \n" + 
				"MANY GRAM POSITIVE RODS \n" + 
				"MANY GRAM NEGATIVE RODS \n" + 
				"MODERATE GRAM POSITIVE COCCI \n" + 
				"RARE BUDDING YEAST \n" + 
				"FINAL REPORT                            11/13/04  1018 \n" + 
				"ESCHERICHIA COLI 4+ \n" + 
				"PROTEUS MIRABILIS 2+ \n" + 
				"ENTEROCOCCUS SPECIES 2+ \n" + 
				"CITROBACTER FREUNDII 1+ \n" + 
				"MIXED WITH NORMAL SKIN FLORA 4+ \n" + 
				"E COLI                         MIC MIC INTERP \n" + 
				"______                         ___ __________ \n" + 
				"AMIKACIN               <=2          S \n" + 
				"CEFEPIME               <=4          S \n" + 
				"CEFTRIAXONE            <=8          S \n" + 
				"CEPHALOTHIN              8          S \n" + 
				"CIPROFLOXACIN          >=4          R \n" + 
				"ERTAPENEM            0.016          S \n" + 
				"GENTAMICIN            >=16          R \n" + 
				"IMIPENEM               <=4          S \n" + 
				"MOXIFLOXACIN           >32          R \n" + 
				"PIPER/TAZOBACT         <=8          S \n" + 
				"SXT                    160          R \n" + 
				"TOBRAMYCIN               1          S \n" + 
				"PROMIR                         MIC MIC INTERP \n" + 
				"______                         ___ __________ \n" + 
				"AMIKACIN               <=2          S \n" + 
				"CEFEPIME               <=4          S \n" + 
				"CEFTRIAXONE            <=8          S \n" + 
				"CEPHALOTHIN            <=2          S \n" + 
				"CIPROFLOXACIN        <=0.5          S \n" + 
				"ERTAPENEM            0.012          S \n" + 
				"GENTAMICIN               1          S \n" + 
				"IMIPENEM               <=4          S \n" + 
				"MOXIFLOXACIN             1          S \n" + 
				"PIPER/TAZOBACT         <=8          S \n" + 
				"SXT                   <=10          S \n" + 
				"TOBRAMYCIN               1          S \n" + 
				"C FREUND                       MIC MIC INTERP \n" + 
				"________                       ___ __________ \n" + 
				"AMIKACIN               <=2          S \n" + 
				"CEFEPIME               <=4          S \n" + 
				"CEFTRIAXONE            <=8          S \n" + 
				"CEPHALOTHIN                         R \n" + 
				"CIPROFLOXACIN        <=0.5          S \n" + 
				"ERTAPENEM            0.008          S \n" + 
				"GENTAMICIN           <=0.5          S \n" + 
				"IMIPENEM               <=4          S \n" + 
				"MOXIFLOXACIN         0.064          S \n" + 
				"PIPER/TAZOBACT         <=8          S \n" + 
				"SXT                   <=10          S \n" + 
				"TOBRAMYCIN           <=0.5          S \n" + 
				"ENTERO                         MIC MIC INTERP \n" + 
				"______                         ___ __________ \n" + 
				"AMPICILLIN          <=0.12          S \n" + 
				"VANCOMYCIN           <=0.5          S";
		
		
		s2 = deidentificationRegex.compositeRegex(s, -7, blackListMap, "Urbain,Jay F");
		System.out.println(s + " : " + s2);
		
		String s3 = mrnRegex(s);
		System.out.println(s + " : " + s3);
		
//		//String s3 = compositeRegex(s, 10);
//		System.out.println(s3);
//		String s4 = "414-745-5102 414 745 5102 (414)745-5102 (414) 745-5102 (414)-745-5102 745-5102 7455102";
////		String s4 = "414-745-5102";
//		System.out.println(s4);
//		//String s5 = compositeRegex(s4, -10);
//		System.out.println(s5);

	}
}

