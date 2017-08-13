package com.gboxsw.miniac.timeseries;

/**
 * Exception thrown when sample is older than the last sample in the storage.
 */
public class InvalidTimeOfSampleException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public InvalidTimeOfSampleException() {
		super("Sample is older that the last stored sample.");
	}
}
