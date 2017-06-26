package deidentification.options;

import java.io.File;
import java.sql.Driver;

/**
 * Options required to run the main Deidentification class
 *
 * @author Matt Hoag
 */
public interface DeidOptions {

	// Database Options
	public String getDburl();
	public String getLogin();
	public String getPassword();

	public Class<Driver> registerAndGetDBDriver() throws ClassNotFoundException;
	public String getDBDriver();

	// Run Configuration
	public Integer getNthreads();
	public Integer getRecordsPerThread();

	// Deidentification Config
	public File getWhitelistfile();
	public File getBlacklistfile();
	public Class getNamedentityrecognitionclass();
	public Class getRegexdeidentificationclass();

	// Input/Output
	public String getQuery();
        public String getUpdateQuery();
	public String getDeidnotestablename();
        public boolean isUpdateOnly();
}


