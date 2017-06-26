/**
 * @author jayurbain
 * 
 *  @version 12/28/2013, 9/2/2014
 */
package deidentification;

import java.util.*;

/**
 * Abstract class for defining a NamedEntityRecognition strategy
 * Implementation example: deidentification.mcw.NamedEntityRecgonitionMCW
 * 
 * In addition to creating a constructor, the primary method to implement
 * is: String performAnnotation(String text). Which given a String,
 * performs named entity annotation.
 */
public abstract class NamedEntityRecognition {

	protected Map<String, String> whiteListMap = new HashMap<String, String>();
	protected Map<String, String> blackListMap = new HashMap<String, String>();

	public abstract String performAnnotation(String text);

	public Map<String, String> getWhiteListMap() {
		return whiteListMap;
	}

	public void setWhiteListMap(Map<String, String> whiteListMap) {
		this.whiteListMap = whiteListMap;
	}

	public Map<String, String> getBlackListMap() {
		return blackListMap;
	}

	public void setBlackListMap(Map<String, String> blackListMap) {
		this.blackListMap = blackListMap;
	}
}
