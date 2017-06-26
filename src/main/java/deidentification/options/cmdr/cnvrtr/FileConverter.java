package deidentification.options.cmdr.cnvrtr;

import com.beust.jcommander.IStringConverter;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * @author Matt Hoag
 */
public class FileConverter implements IStringConverter<File> {

	public File convert(String value) { return new File(value);	}
}
