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
