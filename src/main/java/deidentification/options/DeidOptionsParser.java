package deidentification.options;

/**
 * Interface for parsing the DeidOptions
 *
 * @author Matt Hoag
 */
public interface DeidOptionsParser {

	/**
	 * Constructs a new Exception indicating that the parsed options are incompatible with running the main program
	 */
	class InvalidOptions extends Exception{
		private final String helpText;
		private final int exitCode;

		public InvalidOptions(String helpText) { this(null, helpText, 0); }
		public InvalidOptions(Exception cause, String helpText){ this (cause, helpText, 1); }
		public InvalidOptions(Exception cause, String helpText, int exitCode) {
			super(cause);
			this.helpText = helpText;
			this.exitCode = exitCode;
		}

		/**
		 * Help Text displaying how a user should construct their command line arguments
		 *
		 * @return help text
		 */
		public String getHelpText() { return helpText; }

		/**
		 * Code used to indicate the exit condition of the run
		 * (e.g. if the user asked for "help text" exit code would be 0 otherwise >0)
		 *
		 * @return exit code
		 */
		public int getExitCode() { return exitCode; }

	}

	/**
	 * Parses command line arguments and generates DeidOptions
	 *
	 * @param argv the command line arguments
	 * @return the parsed run options
	 * @throws deidentification.options.DeidOptionsParser.InvalidOptions thrown if the command line
	 * arguments indicate "help"
	 */
	public DeidOptions parseOptions(String[] argv) throws InvalidOptions;
}
