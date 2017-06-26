/**
 * 
 */
package deidentification;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jay Urbain
 *
 * @version 12/28/2013, 9/2/2014
 */
public class MedicalRecordWrapper {
	
	String id;
	String noteId;
	String text;
	String regexText;
	String deIdText; // after regexText
	int dateOffset;
	long millisecondsNER;
	long millisecondsRegex;
	String patientName;
	
	String patientNumber;
	String encounterNumber;
	String date;
	
	
	/**
	 * Wrapper for medical record constructor
	 * 
	 * @param id - unique record identifier (DB auto-increment id)
	 * @param noteId - original note identifier (could be duplicate in our DB since records are split into one or more lines
	 * @param text - original record tet
	 * @param regexText - test post regular expression execution
	 * @param deIdText - final de-identification text
	 */
	
	public MedicalRecordWrapper(String id, String noteId, String text, String regexText,
			String deIdText, int dateOffset, String patientName) {
		super();
		this.id = id;
		this.noteId = noteId;
		this.text = text;
		this.regexText = regexText;
		this.deIdText = deIdText;
		this.dateOffset = dateOffset;
		this.patientName = patientName;
	}

	/**
	 * @return - unique record identifier (DB auto-increment id)
	 */
	public String getId() {
		return id;
	}

	/**
	 * @return - unique record identifier (DB auto-increment id)
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return - original record identifier, not necessarily unique
	 */
	public String getNoteId() {
		return noteId;
	}

	/**
	 * @param noteId - original record identifier, not necessarily unique
	 */
	public void setNoteId(String noteId) {
		this.noteId = noteId;
	}

	/**
	 * @return - original text to be de-identified
	 */
	public String getText() {
		return text;
	}

	/**
	 * @return String - original text to be de-identified
	 */
	public void setText(String text) {
		this.text = text;
	}

	/**
	 * @return String - text after regex processing, but before NER
	 */
	public String getRegexText() {
		return regexText;
	}

	/**
	 * @param regexText - text after regex processing, but before NER
	 */
	public void setRegexText(String regexText) {
		this.regexText = regexText;
	}

	/**
	 * @return - de-identifed text
	 */
	public String getDeIdText() {
		return deIdText;
	}

	/**
	 * @param deIdText - de-identifed text
	 */
	public void setDeIdText(String deIdText) {
		this.deIdText = deIdText;
	}

	/**
	 * @return - data offset used for de-id
	 */
	public int getDateOffset() {
		return dateOffset;
	}

	/**
	 * param dateOffset - date offset applied for data de-id
	 */
	public void setDateOffset(int dateOffset) {
		this.dateOffset = dateOffset;
	}

	/**
	 * @return - unique record identifier (DB auto-increment id)
	 */
	public long getMillisecondsNER() {
		return millisecondsNER;
	}

	/**
	 * param millisecondsNER - NER (named entity recognition) execution time in msec
	 */
	public void setMillisecondsNER(long millisecondsNER) {
		this.millisecondsNER = millisecondsNER;
	}

	/**
	 * return - regex execution time in msec
	 */
	public long getMillisecondsRegex() {
		return millisecondsRegex;
	}

	/**
	 * param millisecondsRegex - regex execution time in msec
	 */
	public void setMillisecondsRegex(long millisecondsRegex) {
		this.millisecondsRegex = millisecondsRegex;
	}

	public String getPatientName() {
		return patientName;
	}

	public void setPatientName(String patientName) {
		this.patientName = patientName;
	}

	public String getPatientNumber() {
		return patientNumber;
	}

	public void setPatientNumber(String patientNumber) {
		this.patientNumber = patientNumber;
	}

	public String getEncounterNumber() {
		return encounterNumber;
	}

	public void setEncounterNumber(String encounterNumber) {
		this.encounterNumber = encounterNumber;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	/**
	 * @return - formatted String of class
	 */
	public String toString() {
		
		return 
				id + " | " +
				noteId + " | " +
				dateOffset + " | " +
				text + " | " +
				deIdText + " | " +
				millisecondsNER + " | " +
				millisecondsRegex + " | " +
				patientName;
	}

}
