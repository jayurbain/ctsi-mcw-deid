/**
 * 
 */
package deidentification;

import java.util.List;
import java.util.Map;


/**
 * @author jayurbain
 *
 * Interface for de-identification regular expressions
 * Where to handle MRN, dates with dateOffset, email, phone, address
 */
public interface DeidentificationRegex {

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

	public String compositeRegex(String s, int dateOffset, Map<String, String> blackListMap, String patientName );

}

