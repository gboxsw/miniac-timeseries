package com.gboxsw.miniac.timeseries;

import java.io.*;
import java.util.*;

import java.nio.channels.Channels;
import java.nio.channels.FileLock;

/**
 * Simple file-based storage for time series data. The implementation is not
 * thread-safe.
 */
public class TimeSeriesStorage {

	/**
	 * Maximal number of decimal places after decimal point of a item value
	 * supported by the storage.
	 */
	public static final int MAX_DECIMALS = 10;

	// -------------------------------------------------------------------------
	// Exceptions
	// -------------------------------------------------------------------------

	/**
	 * Exception thrown when sample is older than the last sample in the
	 * storage.
	 */
	public static class InvalidTimeOfSampleException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		
		public InvalidTimeOfSampleException() {
			super("Sample is older that the last stored sample.");
		}
	}

	/**
	 * Exception thrown when concurent modification of storage file is detected.
	 */
	public static class ConcurentModificationException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public ConcurentModificationException() {
			super("Concurent writing to storage file (storage file was modified in meantime).");
		}
	}

	// -------------------------------------------------------------------------
	// internal class Item - details about an item forming a sample
	// -------------------------------------------------------------------------

	/**
	 * Details about a item (of a sample)
	 */
	private static class Item {
		/**
		 * Name of item.
		 */
		final String name;

		/**
		 * Number of decimal digits after decimal point.
		 */
		final byte decimals;

		/**
		 * Power of ten used to transform floats to integers.
		 */
		final long decimalsPower;

		/**
		 * Constructs new item.
		 * 
		 * @param name
		 *            the name of item.
		 * @param decimals
		 *            the number of decimals after decimal point.
		 */
		Item(String name, byte decimals) {
			this.name = name;
			this.decimals = (byte) Math.min(Math.max(decimals, 0), MAX_DECIMALS);

			long power = 1;
			for (int i = 0; i < decimals; i++) {
				power *= 10;
			}

			this.decimalsPower = power;
		}
	}

	// -------------------------------------------------------------------------
	// internal class Sample - a sample
	// -------------------------------------------------------------------------

	/**
	 * Sample stored in the storage.
	 */
	private static class Sample {
		/**
		 * Time of the sample.
		 */
		final long time;

		/**
		 * Raw (integer) values of items.
		 */
		final long[] values;

		/**
		 * Constructs new record for storing a sample.
		 */
		Sample(long time, long[] values) {
			this.time = time;
			this.values = values;
		}
	}

	// -------------------------------------------------------------------------
	// public class Reader - iterator over samples stored in storage
	// -------------------------------------------------------------------------

	/**
	 * Time series reader that encapsulates reading functionality of the
	 * storage.
	 */
	public class Reader implements Closeable {

		/**
		 * Data input stream used for reading samples.
		 */
		private DataInputStream dataInput;

		/**
		 * File lock.
		 */
		final private FileLock fileLock;

		/**
		 * The time of the current sample.
		 */
		private long sampleTime;

		/**
		 * Raw values of the current sample.
		 */
		private final long[] sampleValues;

		/**
		 * Heading (intro) byte of next sample
		 */
		private int nextIntroByte = -1;

		/**
		 * Constructs new time-series reader.
		 * 
		 * @throws IOException
		 *             if reading of the input failed.
		 */
		private Reader() throws IOException {
			sampleTime = -1;
			sampleValues = new long[items.length];
			FileInputStream fis = new FileInputStream(storageFile);
			fileLock = fis.getChannel().tryLock(0, Long.MAX_VALUE, true);
			if (fileLock == null) {
				nextIntroByte = -1;
				fis.close();
				throw new IOException("Storage file (" + storageFile + ") is locked.");
			}

			dataInput = new DataInputStream(new BufferedInputStream(fis));
			dataInput.skip(dataOffset);

			try {
				nextIntroByte = dataInput.readUnsignedByte();
			} catch (Exception e) {
				nextIntroByte = -1;
			}
		}

		/**
		 * Returns whether there is next sample.
		 * 
		 * @return true, if there is next sample that can be read, false
		 *         otherwise.
		 */
		public boolean hasNext() {
			return nextIntroByte >= 0;
		}

		/**
		 * Moves to next sample.
		 * 
		 * @throws IOException
		 *             if reading failed.
		 */
		public void next() throws IOException {
			if (!hasNext()) {
				throw new IOException("End of samples.");
			}

			// read sample
			boolean differentialEncoding = (nextIntroByte & 0x80) > 0;
			if (differentialEncoding) {
				sampleTime += readVLong(dataInput);
				for (int i = 0; i < sampleValues.length; i++) {
					sampleValues[i] += readVLong(dataInput);
				}
			} else {
				sampleTime = readVLong(dataInput);
				for (int i = 0; i < sampleValues.length; i++) {
					sampleValues[i] = readVLong(dataInput);
				}
			}

			// read intro byte of next sample
			try {
				nextIntroByte = dataInput.readUnsignedByte();
			} catch (Exception e) {
				nextIntroByte = -1;
			}
		}

		/**
		 * Returns time of the current sample.
		 * 
		 * @return the time of the current sample.
		 * @throws RuntimeException
		 *             if no sample is available.
		 */
		public long getTime() throws RuntimeException {
			if (sampleTime < 0) {
				throw new RuntimeException("No sample is available.");
			}

			return sampleTime;
		}

		/**
		 * Returns item values of the current sample.
		 * 
		 * @return the map with item values.
		 * @throws RuntimeException
		 *             if no sample is available.
		 */
		public Map<String, Number> getValues() {
			if (sampleTime < 0) {
				throw new RuntimeException("No sample is available.");
			}

			Map<String, Number> result = new HashMap<String, Number>();
			for (int i = 0; i < items.length; i++) {
				if (items[i].decimals == 0) {
					result.put(items[i].name, new Long(sampleValues[i]));
				} else {
					result.put(items[i].name, new Double(((double) sampleValues[i]) / items[i].decimalsPower));
				}
			}

			return result;
		}

		@Override
		public void close() throws IOException {
			if (dataInput == null) {
				return;
			}

			sampleTime = -1;
			try {
				fileLock.release();
			} finally {
				DataInputStream diToClose = dataInput;
				dataInput = null;
				diToClose.close();
			}
		}
	}

	// -------------------------------------------------------------------------
	// public class Writer - configurable writer of samples
	// -------------------------------------------------------------------------

	/**
	 * Writer of samples to storage.
	 */
	public class Writer implements Closeable {
		/**
		 * Storage file that is kept open.
		 */
		private RandomAccessFile openStorageFile = null;

		/**
		 * Exclusive file lock related to storage file that is kept open.
		 */
		private FileLock fileLock = null;

		/**
		 * Time of the last sample added to storage by this writer. Each added
		 * sample must increase this time.
		 */
		private long storageSampleTime;

		/**
		 * The last sample or null, if no sample was stored by the writer.
		 */
		private Sample lastStoredSample;

		/**
		 * Time, when the last flush of samples occurred.
		 */
		private long timeOfLastFlush = 0;

		/**
		 * Maximal number of unflushed samples.
		 */
		private int maxUnflushedSamples = 10;

		/**
		 * Maximal time in seconds between two flushes.
		 */
		private long maxSecondsBetweenFlushes = 15 * 60;

		/**
		 * List of samples that are not stored in the associated storage file.
		 */
		private final List<Sample> unflushedSamples = new ArrayList<Sample>();

		/**
		 * Returns the maximal number of unflushed samples.
		 * 
		 * @return the maximal number of unflushed samples.
		 */
		public int getMaxUnflushedSamples() {
			return maxUnflushedSamples;
		}

		/**
		 * Sets the maximal number of unflushed samples.
		 * 
		 * @param maxUnflushedSamples
		 *            the desired maximal number of unflushed samples.
		 */
		public void setMaxUnflushedSamples(int maxUnflushedSamples) {
			this.maxUnflushedSamples = maxUnflushedSamples;
		}

		/**
		 * Returns the maximal time in seconds between two flushes.
		 * 
		 * @return the maximal time in seconds between two flushes.
		 */
		public long getMaxSecondsBetweenFlushes() {
			return maxSecondsBetweenFlushes;
		}

		/**
		 * Sets the maximal time in seconds between two flushes invoked during
		 * inserting new sample to the storage.
		 * 
		 * @param maxSecondsBetweenFlushes
		 *            the maximal time in seconds between two flushes.
		 */
		public void setMaxSecondsBetweenFlushes(long maxSecondsBetweenFlushes) {
			this.maxSecondsBetweenFlushes = maxSecondsBetweenFlushes;
		}

		/**
		 * Private constructor of a storage writer.
		 */
		private Writer() {
			storageSampleTime = storageTime;
			timeOfLastFlush = MonotonicClock.INSTANCE.currentTimeMillis();
		}

		/**
		 * Opens the storage file and keeps it open.
		 * 
		 * @throws IOException
		 *             if opening of the storage file failed.
		 */
		public void open() throws IOException {
			if (openStorageFile != null) {
				return;
			}

			openStorageFile = new RandomAccessFile(storageFile, "rw");
			try {
				fileLock = openStorageFile.getChannel().tryLock();
				if (fileLock == null) {
					throw new IOException("Storage file (" + storageFile + ") is locked.");
				}
			} catch (Exception e) {
				try {
					openStorageFile.close();
				} finally {
					openStorageFile = null;
				}
			}
		}

		/**
		 * Flushes all samples and closes the storage file.
		 * 
		 * @throws IOException
		 *             if closing of the storage file failed.
		 */
		public void close() throws IOException {
			try {
				flush();
			} finally {
				if (openStorageFile != null) {
					RandomAccessFile raf = openStorageFile;
					openStorageFile = null;
					try {
						fileLock.release();
					} finally {
						fileLock = null;
						raf.close();
					}
				}
			}
		}

		/**
		 * Adds new sample to storage and flushes unflushed samples if
		 * necessary.
		 * 
		 * @param time
		 *            the time of sample (arbitrary increasing non-negative
		 *            value)
		 * @param values
		 *            the map with values of items forming the sample.
		 * @throws IOException
		 *             if writing the sample failed.
		 */
		public void addSample(long time, Map<String, ? extends Number> values) throws IOException {
			// check time of sample
			if (time <= storageSampleTime) {
				throw new InvalidTimeOfSampleException();
			}

			// create array with raw values (index 0 stores time)
			long[] rawValues = new long[items.length];
			for (int i = 0; i < items.length; i++) {
				Number value = values.get(items[i].name);
				if (value == null) {
					throw new NullPointerException("Value of the item \"" + items[i].name + "\" is not defined.");
				}

				rawValues[i] = Math.round(value.doubleValue() * items[i].decimalsPower);
			}

			// add sample to storage
			unflushedSamples.add(new Sample(time, rawValues));
			storageSampleTime = time;

			// if necessary, flush samples to storage file
			long secondsFromLastFlush = (MonotonicClock.INSTANCE.currentTimeMillis() - timeOfLastFlush) / 1000;
			if ((unflushedSamples.size() > maxUnflushedSamples) || (secondsFromLastFlush < 0)
					|| (secondsFromLastFlush > maxSecondsBetweenFlushes)) {
				flush();
			}
		}

		/**
		 * Flushes all unflushed samples to underlying storage file. To complete
		 * the operation, the storage file can be closed (it will be opened and
		 * closed during flushing).
		 * 
		 * @throws IOException
		 *             if writing samples failed.
		 */
		public void flush() throws IOException {
			// store time of the last flush
			timeOfLastFlush = MonotonicClock.INSTANCE.currentTimeMillis();

			// check whether there is a sample to be flushed
			if (unflushedSamples.isEmpty()) {
				return;
			}

			RandomAccessFile raf = null;
			FileLock localFileLock = null;
			try {
				// prepare storage file
				if (openStorageFile != null) {
					raf = openStorageFile;
				} else {
					raf = new RandomAccessFile(storageFile, "rw");
					localFileLock = raf.getChannel().tryLock();
					if (localFileLock == null) {
						throw new IOException("Storage file (" + storageFile + ") is locked.");
					}
				}

				// after reopening storage file, realize some checks
				if (openStorageFile != raf) {
					// check whether we opened a correct storage file (with
					// respect to its ID)
					if (raf.readInt() != storageID) {
						throw new ConcurentModificationException();
					}

					// check whether all stored samples are older than samples
					// that we want to store
					long loadedTimeOfLastSample = raf.readLong();
					if (loadedTimeOfLastSample >= unflushedSamples.get(0).time) {
						throw new ConcurentModificationException();
					}

					// if there was another write to storage in the meantime,
					// invalidate lastStoredSample (we don't have current values
					// for differential encoding)
					if ((lastStoredSample != null) && (lastStoredSample.time != loadedTimeOfLastSample)) {
						lastStoredSample = null;
					}
				}

				// move write cursor to end of file
				raf.seek(raf.length());

				// store all unflushed samples
				DataOutputStream out = new DataOutputStream(
						new BufferedOutputStream(Channels.newOutputStream(raf.getChannel())));
				for (Sample sample : unflushedSamples) {
					writeSample(out, sample);
				}
				unflushedSamples.clear();
				out.flush();

				// store time of the last stored sample
				raf.seek(4 /* int:id */);
				raf.writeLong(lastStoredSample.time);
				raf.seek(raf.length());

				// update metadata of storage
				storageTime = lastStoredSample.time;
			} finally {
				if (localFileLock != null) {
					try {
						localFileLock.release();
					} catch (Exception ignore) {
						// ignored exception
					}
				}

				if ((openStorageFile == null) && (raf != null)) {
					raf.close();
				}
			}
		}

		/**
		 * Writes sample to data output.
		 * 
		 * @param out
		 *            the data output.
		 * @param sample
		 *            the sample to be stored.
		 * @throws IOException
		 *             if writing bytes to data output failed.
		 */
		private void writeSample(DataOutput out, Sample sample) throws IOException {
			// first byte introducing a serialized sample
			int introByte = 0;

			// serialize sample
			if (lastStoredSample == null) {
				out.writeByte(introByte);
				writeVLong(out, sample.time);
				for (long value : sample.values) {
					writeVLong(out, value);
				}
			} else {
				// set flag indicating use of differential encoding
				introByte = introByte | 0x80;

				out.writeByte(introByte);
				writeVLong(out, sample.time - lastStoredSample.time);
				for (int i = 0; i < sample.values.length; i++) {
					writeVLong(out, sample.values[i] - lastStoredSample.values[i]);
				}
			}

			// store reference to last stored sample for differential encoding
			lastStoredSample = sample;
		}
	}

	// -------------------------------------------------------------------------
	// Instance variables that store the most important metadata of the storage
	// -------------------------------------------------------------------------

	/**
	 * Storage file.
	 */
	private final File storageFile;

	/**
	 * Storage identifier.
	 */
	private final int storageID;

	/**
	 * Offset of data block in the storage file.
	 */
	private final int dataOffset;

	/**
	 * Items forming samples in stored time series.
	 */
	private final Item[] items;

	/**
	 * Time of the last sample (in time of reading metadata)
	 */
	private long storageTime;

	/**
	 * Opens an existing time-series storage and reads its metadata.
	 * 
	 * @param file
	 *            the storage file.
	 * @throws IOException
	 *             it reading the file failed.
	 */
	public TimeSeriesStorage(File file) throws IOException {
		storageFile = file;
		try (FileInputStream fis = new FileInputStream(storageFile)) {
			FileLock lock = fis.getChannel().tryLock(0, Long.MAX_VALUE, true);
			if (lock == null) {
				throw new IOException("File " + storageFile.getAbsolutePath() + " is locked.");
			}

			try {
				DataInputStream in = new DataInputStream(new BufferedInputStream(fis));

				// read storage ID
				storageID = in.readInt();

				// read time of the last stored sample
				storageTime = in.readLong();

				// read file offset of data block
				dataOffset = in.readInt();

				// read structure of items
				items = readItems(in);
			} finally {
				if (lock != null) {
					lock.release();
				}
			}
		}
	}

	/**
	 * Reads description of items.
	 * 
	 * @param in
	 *            the data input.
	 * 
	 * @return array with loaded details about items.
	 * @throws IOException
	 *             if read failed.
	 */
	private Item[] readItems(DataInput in) throws IOException {
		// read number of items
		int itemsCount = in.readShort();
		if (itemsCount < 0) {
			throw new IOException("Inconsistent data input.");
		}

		// read description of data items
		Item[] result = new Item[itemsCount];
		for (int i = 0; i < result.length; i++) {
			String name = in.readUTF();
			int decimals = in.readUnsignedByte();
			result[i] = new Item(name, (byte) decimals);
		}

		return result;
	}

	/**
	 * Creates and returns new reader of samples stored in the storage.
	 * 
	 * @return the new reader of samples.
	 * @throws IOException
	 *             if reading of storage file failed
	 */
	public Reader newReader() throws IOException {
		return new Reader();
	}

	/**
	 * Creates and returns new writer of samples.
	 * 
	 * @param keepOpen
	 *            by setting true, a newly created will be open and kept open.
	 * @return the new writer of samples.
	 * @throws IOException
	 *             if writing to storage file failed.
	 */
	public Writer newWriter(boolean keepOpen) throws IOException {
		Writer result = new Writer();
		if (keepOpen) {
			result.open();
		}
		return result;
	}

	/**
	 * Creates and returns new writer of samples. The created writer is not kept
	 * open.
	 * 
	 * @return the new writer of samples.
	 * @throws IOException
	 *             if writing to storage file failed.
	 */
	public Writer newWriter() throws IOException {
		return newWriter(false);
	}

	/**
	 * Creates a new (empty) storage with predefined items of samples.
	 * 
	 * @param file
	 *            the file of data storage.
	 * @param items
	 *            the map where keys corresponds to names of item of samples.
	 *            Value (allowed range 0..10) assigned to a key in the map
	 *            specifies the number of decimal places after decimal point
	 *            used for rounding item values.
	 * @throws IOException
	 *             if creation of storage failed.
	 */
	public static void create(File file, Map<String, Integer> items) throws IOException {
		try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(Channels.newOutputStream(raf.getChannel())));

			// generate and store random storage id
			out.writeInt((int) (Math.random() * Integer.MAX_VALUE));

			// time of the last stored sample
			out.writeLong(-1);

			// placeholder for data offset
			out.writeInt(0);

			// write details about items forming a sample
			out.writeShort(items.size());
			for (Map.Entry<String, Integer> entry : items.entrySet()) {
				out.writeUTF(entry.getKey());
				int decimals = entry.getValue();
				decimals = Math.min(Math.max(decimals, 0), MAX_DECIMALS);
				out.writeByte(decimals);
			}

			// flush generated output
			out.flush();

			// write data offset (=total length of header bytes)
			raf.seek(4 /* int:id */ + 8 /* long:time */);
			raf.writeInt((int) raf.length());
		}
	}

	// -------------------------------------------------------------------------
	// Helper methods for reading and writing long values using variable-length
	// encoding.
	// -------------------------------------------------------------------------

	/**
	 * Writes variable length long value to the storage file. If the output is
	 * null, only the size of encoded value is returned.
	 * 
	 * @param out
	 *            the data output.
	 * @param value
	 *            the value to be written.
	 * 
	 * @return the size of encoded value in bytes.
	 * @throws IOException
	 *             if write failed.
	 */
	private static int writeVLong(DataOutput out, long value) throws IOException {
		if (value == Long.MIN_VALUE) {
			if (out != null) {
				out.writeByte(0x40);
			}

			return 1;
		}

		// count of written bytes
		int writtenBytes = 0;

		// handle first byte
		int outByte = 0;
		if (value < 0) {
			// add "NEGATIVE" flag
			outByte = 0x40;
			value = -value;
		}

		outByte += value % 64;
		value = value / 64;

		// add "NEXT BYTE" flag (if necessary)
		if (value > 0) {
			outByte = outByte | 0x80;
		}

		// write first byte
		if (out != null) {
			out.writeByte(outByte);
		}
		writtenBytes++;

		// process all other bytes
		while (value > 0) {
			outByte = (int) (value % 128);
			value = value / 128;
			// add "NEXT BYTE" flag
			if (value > 0) {
				outByte = outByte | 0x80;
			}

			if (out != null) {
				out.writeByte(outByte);
			}

			writtenBytes++;
		}

		return writtenBytes;
	}

	/**
	 * Reads variable length long value to the storage file.
	 * 
	 * @param in
	 *            the data input.
	 * @return the retrieved value.
	 * @throws IOException
	 *             if read failed.
	 */
	private static long readVLong(DataInput in) throws IOException {
		int inByte = in.readUnsignedByte();
		if (inByte == 0x40) {
			return Long.MIN_VALUE;
		}

		long result = 0;

		boolean negativeValue = (inByte & 0x40) > 0;
		boolean hasNextByte = (inByte & 0x80) > 0;
		result = inByte & 0x3F;

		long power = 64;
		while (hasNextByte) {
			inByte = in.readUnsignedByte();
			hasNextByte = (inByte & 0x80) > 0;
			result += (inByte & 0x7F) * power;
			power *= 128;
		}

		if (negativeValue) {
			result = -result;
		}

		return result;
	}
}
