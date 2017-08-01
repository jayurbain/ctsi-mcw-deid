# ctsi-mcw-deid
NLP Software to deidentify protected health information from clinical notes.  

Jay Urbain, PhD  
jay.urbain@gmail.com

##### 1) Use case: Read identified records from tab-delimited file system (local host or HDFS), write deidentified tab-delimited records to file system (local host or HDFS). Runs deidentificaiton software as part of this application.

Example:
java -jar code/ctsi_mcw_deid_Deidentification.jar data/flint/hadoop_query_1_1_short.tsv data/flint/hadoop_query_1_1_short_deid.tsv code/blacklist.txt deidentification.mcw.NamedEntityRecognitionMCW deidentification.mcw.DeidentificationRegexMCW 1000 8 george

Parameters for main class: deidentification.Deidentification
- input_file
- output_file
- blacklist (text file for words to absolutely cross out)
- Named entity class (provided)
- Regular expression class (provided)
- Number of records to process per thread at a time
- Number of threads
- george (optional parameter for george format described below)

##### Input and output file formats.

George format:
Input -> (include header: id, pat_first_name, pat_last_name, date_shift, text)
- id // id used to correlate input (identified records) with output (deidentified records)
- pat_first_name // provide patient name if available (backup to named entity identification NLP)
- pat_last_name  // provide patient name
- date_shift // Number of days (positive or negative to uniformly shift dates), e.g., -10, +15
- text // text to deidentify
Output -> (id, text)

Search engine format:
Input -> (no header: id, pat_id_enc, date_shift, patient_num, pat_enc_csn_id, encounter_num, pat_first_name, pat_last_name, contact_date, note_type, text)
- id // id used to correlate input (identified records) with output (deidentified records)
- pat_id_enc // internal id used for tracability
- date_shift // Number of days (positive or negative to uniformly shift dates), e.g., -10, +15
- patient_num // passed through, used as deidentified patient number
- pat_enc_csn_id // internal id used for tracability
- encounter_num // passed through, used as deidentified encounter number
- pat_first_name // provide patient name if available (backup to named entity identification NLP)
- pat_last_name  // provide patient name
- contact_date // date of encounter, will be date shifted
- note_type // E.g., "progress_note"
- text // text to deidentify
Output -> (patient_number, encounter_num, contact_date, text)

##### Main class processing: deidentification.Deidentification
1) Blacklist processing (deidentification.mcw.DeidentificationRegexMCW) 
2) Pre-processing and regular expression processing. MCW implementation 
3) deidentification.mcw.DeidentificationRegexMCW) removes invalid character encodings,
places spaces between mixed capitalization terms and mixed alpha numerics; and
de-id's dates, MRN/ids, phone, email, and addresses.
4) Named entity recognition for identification and de-id of person and location entity types. MCW implementation
(deidentification.mcw.NamedEntityRecognitionMCW) uses 3 Stanford NLP named entity models trained are different text repositories. Entiites are replaced with [XXXXX]
5) Output fully de-id'd text with identifier included with input file to tab-delimited output file (no header)

##### 2) Use case: Read identifed records from tab-delimited file system (local host or HDFS), write deidentified tab-delimited records to file system (local host or HDFS). Sumbits record to be deidentified to web service in JSON format. Execute as MapReduce 2.0/YARN application.    
     
##### 3) Use case: Read identifed records from tab-delimited file system (local host or HDFS), write deidentified tab-delimited records to file system (local host or HDFS). Sumbits record to be deidentified to web service in JSON format. Assumes the following web service is running.
Only search engine input format is supported
 
- [Deidentification Web service](https://github.com/jayurbain/ctsi-mcw-deid-service.git)   

### License
"CTSI MCW Deidentification" is licensed under the GNU General Public License (v3 or later; in general "CTSI MCW Deidentification" code is GPL v2+, but "CTSI MCW Deidentification" uses several Apache-licensed libraries, and so the composite is v3+). Note that the license is the full GPL, which allows many free uses, but not its use in proprietary software which is distributed to others. For distributors of proprietary software, "CTSI MCW Deidentification" is also available from CTSI of Southeast Wisconsin under a commercial licensing You can contact us at jay.urbain@gmail.com. 
