/**
 * @author Jay Urbain
 *
 * @version 12/28/2013, 9/2/2014
 */

package deidentification;

import java.util.ArrayList;
import java.util.List;

import deidentification.NamedEntityRecognition;
import util.SimpleIRUtilities;

/**
 * @author jayurbain
 *
 * Processing thread for executing the de-identification process.
 * Takes NamedEntityRecogition class at construction.
 * Processes set of MedicalRecordWrapper set in List<MedicalRecordWrapper> recordList
 */
public class DeIdentificationThread extends Thread {
	
	NamedEntityRecognition namedEntityRecognition;
	List<MedicalRecordWrapper> recordList;

	/**
	 * @param namedEntityRecognition Named entity recognition class
	 */
	public DeIdentificationThread(
			NamedEntityRecognition namedEntityRecognition) {
		super();
		this.namedEntityRecognition = namedEntityRecognition;
		this.recordList = new ArrayList<MedicalRecordWrapper>();
	}

	/**
	 * Thread processing. Read records set in recordList, 
	 * execute namedEntityRecognition.performAnnotation on record.
	 */
	@Override
	public void run() {

		for( MedicalRecordWrapper r : recordList ) {
			java.util.Date startDate = new java.util.Date();
			r.setDeIdText( namedEntityRecognition.performAnnotation( r.getRegexText() ) );
			java.util.Date endDate = new java.util.Date();
			long msecs = SimpleIRUtilities.getElapsedTimeMilliseconds(startDate, endDate);
			r.setMillisecondsNER(msecs);
		}
	}

	/**
	 * @ return NamedEntityRecognition
	 */
	public NamedEntityRecognition getNamedEntityRecognition() {
		return namedEntityRecognition;
	}

	/**
	 * @param getNamedEntityRecognition 
	 */
	public void setNamedEntityRecognition(
			NamedEntityRecognition namedEntityRecognition) {
		this.namedEntityRecognition = namedEntityRecognition;
	}

	/**
	 * @ return List<MedicalRecordWrapper>
	 */
	public List<MedicalRecordWrapper> getRecordList() {
		return recordList;
	}

	/**
	 * @param List<MedicalRecordWrapper> 
	 */
	public void setRecordList(List<MedicalRecordWrapper> recordList) {
		this.recordList = recordList;
	}
}
