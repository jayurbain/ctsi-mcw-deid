package deidentification.options.cmdr;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import deidentification.options.DeidOptions;
import deidentification.options.DeidOptionsParser;
import deidentification.options.cmdr.cnvrtr.ClassConverter;
import deidentification.options.cmdr.cnvrtr.FileConverter;

import java.io.File;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Matt Hoag
 */
public class DeidOptionsJCmd implements DeidOptions, DeidOptionsParser {

	@Parameter
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {"--help", "-h"}, description = "Print this help text", help = true)
	private boolean help;

	@Parameter(names = "-dburl", description = "Database URL")
	private String dburl = "jdbc:mysql://localhost/clarity";
	public String getDburl(){return dburl;}

	@Parameter(names = "-login", description = "Database login name")
	private String login;
	public String getLogin() {return login;}

	@Parameter(names = "-password", description = "Database password")
	private String password;
	public String getPassword() {return password;}

	@Parameter(names ="-dbdriver", description = "Database driver. Note: only MySQL has been validated")
	private String dbdriver = "com.mysql.jdbc.Driver";
	public Class<Driver> registerAndGetDBDriver() throws ClassNotFoundException {
		return (Class<Driver>) Class.forName(dbdriver);
	}
	public String getDBDriver() {
		return dbdriver;
	}

	@Parameter(names = "-nthreads", description = "Number of concurrent threads for processing records.")
	private Integer nthreads = 1;
	public Integer getNthreads() {return nthreads;}

	@Parameter(names = "-recordsperthread", description = "Number of records assigned to each thread.")
	private Integer recordsPerThread = 100;
	public Integer getRecordsPerThread() {return recordsPerThread;}

	@Parameter(names = "-whitelistfilename", converter = FileConverter.class,
			description = "text file containing terms to NOT de-identify.")
	private File whitelistfile = new File("whitelist.txt");
	public File getWhitelistfile() {return whitelistfile;}

	@Parameter(names = "-blacklistfilename", converter = FileConverter.class,
			description = "text file containing terms to ALWAYS de-identify.")
	private File blacklistfile = new File("blacklist.txt");
	public File getBlacklistfile() {return blacklistfile;}

	@Parameter(names = "-namedentityrecognitionclass", converter = ClassConverter.class,
			description = "Named entity class. Must implement the \"deidentification.NamedEntityRecognition\" " +
					"abstract class.")
	private Class namedentityrecognitionclass = deidentification.mcw.NamedEntityRecognitionMCW.class;
	public Class getNamedentityrecognitionclass() {return namedentityrecognitionclass;}

	@Parameter(names = "-regexdeidentificationclass", converter = ClassConverter.class,
			description = "Regular expression class. Must implement the " +
					"\"deidentification.DeidentificationRegex\" interface.")
	private Class regexdeidentificationclass = deidentification.mcw.DeidentificationRegexMCW.class;
	public Class getRegexdeidentificationclass() {return regexdeidentificationclass;}

	@Parameter(names = "-query",
			description = "Input query of records to de-identify. Select must be of the following form: " +
					"\n\t\t" + "\"select  id, note_id, note_text, date_off\"")
	private String query = "select (NOTE_CSN_ID*100)+LINE as ID, NOTE_ID as NOTE_ID, NOTE_TEXT as NOTE_TEXT, " +
			"0 as DATE_OFF from clarity.HNO_NOTE_TEXT order by ID";
	public String getQuery() {return query;}

	@Parameter(names = "-deidnotestablename", description = "name of database output table")
	private String deidnotestablename = "DEID_HNO_NOTES";
	public String getDeidnotestablename() {return deidnotestablename;}

	@Parameter(names = "-update-only", description = "Update the database output table, don't created it")
	private boolean updateOnly = false;
	public boolean isUpdateOnly() { return updateOnly; }
        
        @Parameter(names = "-updatequery",
	description = "Update query of records. Update must be of the following form: " +
					"\n\t\t" + "\"update table_name set set deid_note_text=? where id = ? \"")
	private String updatequery = "update table_name set deid_note_text=? where id = ?";
	public String getUpdateQuery() {return updatequery;}

	public DeidOptions parseOptions(String[] argv) throws InvalidOptions {
		final JCommander cmd = new JCommander(this);

		try {
			cmd.parse(argv);
		} catch (Exception e) {
			StringBuilder help = new StringBuilder();
			cmd.usage(help);
			throw new InvalidOptions(e, help.toString());
		}

		if(this.help) {
			StringBuilder help = new StringBuilder();
			cmd.usage(help);
			throw new InvalidOptions(help.toString());
		}

		return this;
	}
}