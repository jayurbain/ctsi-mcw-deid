package deidentification.options.cmdr.cnvrtr;

import com.beust.jcommander.IStringConverter;

/**
 * @author Matt Hoag
 */
public class ClassConverter implements IStringConverter<Class> {

	public class ClassConverterException extends RuntimeException {
		public ClassConverterException(Throwable cause){ super(cause); }
	}

	public Class convert(String value) {
		try {
			return Class.forName(value);
		} catch (ClassNotFoundException e) {
			throw new ClassConverterException(e);
		}
	}
}
