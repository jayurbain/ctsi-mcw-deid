# ctsi-mcw-deid
NLP Software to deidentify protected health information from clinical notes.
Jay Urbain, PhD

##### 1) Use case: Read identifed records from tab-delimited file system (local host or HDFS), write deidentified tab-delimited records to file system (local host or HDFS). Runs deidentificaiton software as part of this application.

Example:
java -jar code/ctsi_mcw_deid_Deidentification.jar data/flint/hadoop_query_1_1_short.tsv data/flint/hadoop_query_1_1_short_deid.tsv code/blacklist.txt deidentification.mcw.NamedEntityRecognitionMCW deidentification.mcw.DeidentificationRegexMCW 1000 8 george

Parameters for main class: deidentification.Deidentification
- input_file
- output_file
- blacklist (text file for words to absolutely cross out)
- Named entity class (provided)
- Regulear expression class (provided)
- Number of records to process per thread at a time
- Number of threads
- george (optional parameter for george format described below)

##### Input and output file formats.

George format:
Input -> (include header: id, pat_first_name, pa