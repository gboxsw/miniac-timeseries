package com.gboxsw.miniac.timeseries;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileLock;
import java.util.*;

public class TimeSeriesStorage {

	/**
	 * Maximal number of decimal places after decimal point of a item value
	 * supported by the storage.
	 */
	public static final int MAX_DECIMALS = 10;

	/**
	 * Value that encodes null (undefined value)
	 */
	private static final long NULL_VALUE = Long.MIN_VALUE;

	/**
	 * Magic bytes in the beginning of the file.
	 */
	private static final int[] MAGIC_BYTES = { 0x94, 0xDC };

	/**
	 * Maximal (default) size of index table.
	 */
	private static final int INDEX_TABLE_SIZE = 10;

	/**
	 * Codes of record types.
	 */
	private static class RecordType {
		/**
		 * Heading byte of the index table.
		 */
		private static final int INDEX_TABLE = 0x01;

		/**
		 * Heading byte of sample.
		 */
		private static final int SAMPLE = 0x02;

		/**
		 * Heading byte of differential sample (content of sample is change of
		 * values with respect to previous sample).
		 */
		private static final int DIF_SAMPLE = 0x03;
	}

	// -------------------------------------------------------------------------
	// internal class Item - details about an item forming a sample
	// -------------------------------------------------------------------------

	/**
	 * Details about an item (of a sample)
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
	// internal class IndexTable - an index table
	// -------------------------------------------------------------------------

	/**
	 * Index table.
	 */
	private class IndexTable {
		/**
		 * Offset of the table.
		 */
		final long offset;

		/**
		 * Level of the index table (index table of level 0 contains offsets of
		 * data blocks).
		 */
		final int level;

		/**
		 * The size of the index table.
		 */
		int size;

		/**
		 * Start times of indexed records.
		 */
		final long[] startTimes;

		/**
		 * Offsets of indexed records
		 */
		final long[] offsets;

		/**
		 * Pre-loaded index tables in the next level (index subtables).
		 */
		final IndexTable[] subtables;

		/**
		 * Constructs the index table.
		 * 
		 * @param offset
		 *            the offset of the index table.
		 * @param level
		 *            the level of the index table.
		 */
		IndexTable(long offset, int level) {
			this.offset = offset;
			this.level = level;
			this.size = 0;

			startTimes = new long[indexTableSize];
			offsets = new long[indexTableSize];
			subtables = new IndexTable[indexTableSize];
		}

		/**
		 * Append new index at the end of the index table.
		 * 
		 * @param startTime
		 *            the start time.
		 * @param offset
		 *            the offset.
		 */
		void appendIndex(long startTime, long offset) {
			if (size >= startTimes.length) {
				throw new IllegalStateException("The index table is full.");
			}

			if (size > 0) {
				if (startTimes[size - 1] >= startTime) {
					throw new IllegalArgumentException(
							"It is not possible to append an index with the same or older time stamp.");
				}
			}

			startTimes[size] = startTime;
			offsets[size] = offset;
			subtables[size] = null;
			size++;
		}

		/**
		 * Returns clone of the index table with modified offset.
		 * 
		 * @param newOffset
		 *            the offset of the returned index table.
		 * @return the cloned index table.
		 */
		IndexTable cloneWithNewOffset(long newOffset) {
			IndexTable result = new IndexTable(newOffset, level);
			System.arraycopy(startTimes, 0, result.startTimes, 0, size);
			System.arraycopy(offsets, 0, result.offsets, 0, size);
			result.size = size;
			return result;
		}

		/**
		 * Returns whether the index table is full.
		 * 
		 * @return true, if the index table is full, false otherwise.
		 */
		boolean isFull() {
			return size == startTimes.length;
		}

		/**
		 * Writes the index table to the storage file.
		 * 
		 * @param storageFile
		 *            the storage file open for writing.
		 * 
		 * @throws IOException
		 *             if the operation failed.
		 */
		private void write(RandomAccessFile storageFile) throws IOException {
			storageFile.seek(offset);

			if (size > indexTableSize) {
				throw new IOException("Invalid index table to be written.");
			}

			DataOutputStream out = new DataOutputStream(
					new BufferedOutputStream(Channels.newOutputStream(storageFile.getChannel())));
			out.writeByte(RecordType.INDEX_TABLE);
			out.writeByte(level);
			out.writeByte(size);

			for (int i = 0; i < size; i++) {
				out.writeLong(startTimes[i]);
				out.writeLong(offsets[i]);
			}

			for (int i = size; i < indexTableSize; i++) {
				out.writeLong(0);
				out.writeLong(0);
			}

			out.flush();
		}
	}

	// -------------------------------------------------------------------------
	// public class Reader - iterator over samples stored in storage
	// -------------------------------------------------------------------------

	/**
	 * Time series reader that encapsulates reading functionality of the
	 * storage.
	 */
	public class SampleReader implements Closeable {

		/**
		 * File lock.
		 */
		private final FileLock fileLock;

		/**
		 * Storage file.
		 */
		private final RandomAccessFile storageFile;

		/**
		 * Data input stream used for reading samples.
		 */
		private DataInputStream dataInput;

		/**
		 * Minimal time of accepted samples.
		 */
		private final long fromTime;

		/**
		 * The time of the current sample.
		 */
		private long sampleTime;

		/**
		 * Raw values of the current sample.
		 */
		private final long[] sampleValues;

		/**
		 * The time of the next sample.
		 */
		private long nextSampleTime;

		/**
		 * Raw values of the next sample.
		 */
		private final long[] nextSampleValues;

		/**
		 * Queue of samples that has been added to the storage after the end of
		 * file has been reached.
		 */
		private Queue<Sample> inMemorySamples;

		/**
		 * Indicates whether reader has been closed.
		 */
		private boolean closed;

		/**
		 * Constructs new time-series reader.
		 * 
		 * @param fromTime
		 *            initial time of reading.
		 * 
		 * @throws IOException
		 *             if reading of the input failed.
		 */
		private SampleReader(long fromTime) throws IOException {
			fromTime = Math.max(0, fromTime);
			this.fromTime = fromTime;

			// initialize data structures
			sampleTime = -1;
			sampleValues = new long[items.length];
			nextSampleTime = -1;
			nextSampleValues = new long[items.length];

			// open file
			storageFile = new RandomAccessFile(file, "r");
			try {
				fileLock = storageFile.getChannel().tryLock(0, Long.MAX_VALUE, true);
				if (fileLock == null) {
					throw new IOException("Storage file (" + file + ") is locked.");
				}

				if (fileSize != storageFile.length()) {
					fileLock.release();
					throw new IOException("Concurrent modification of the storage file.");
				}
			} catch (Exception e) {
				storageFile.close();
				throw e;
			}

			// find initial offset
			long startReadOffset = 0;
			try {
				IndexTable directIndexTable = findDirectIndexTableForTime(storageFile, fromTime);
				int indexPos = findLatestIndexForTime(directIndexTable, fromTime);
				startReadOffset = directIndexTable.offsets[indexPos];
			} catch (Exception e) {
				closeFile();
				throw e;
			}

			// create input stream
			storageFile.seek(startReadOffset);
			dataInput = new DataInputStream(new BufferedInputStream(Channels.newInputStream(storageFile.getChannel())));

			// find first valid record (stored in the file)
			while (fetchNextSampleFromFile()) {
				if (nextSampleTime >= fromTime) {
					break;
				}
			}

			// if no offline sample is found, we should use in-memory samples
			if (nextSampleTime < fromTime) {
				activateInMemorySamples();
			}
		}

		/**
		 * Returns whether there is an available next sample.
		 * 
		 * @return true, if there is next sample that can be read, false
		 *         otherwise.
		 */
		public boolean hasNext() {
			if (closed) {
				return false;
			}

			return (nextSampleTime >= 0) || ((inMemorySamples != null) && (!inMemorySamples.isEmpty()));
		}

		/**
		 * Moves/reads a next sample.
		 * 
		 * @throws IOException
		 *             if reading failed.
		 */
		public void next() throws IOException {
			if (closed) {
				throw new IOException("The reader has been closed.");
			}

			if (!hasNext()) {
				throw new NoSuchElementException("The reader does not contain unread samples.");
			}

			if (nextSampleTime >= 0) {
				sampleTime = nextSampleTime;
				System.arraycopy(nextSampleValues, 0, sampleValues, 0, sampleValues.length);

				if (!fetchNextSampleFromFile()) {
					activateInMemorySamples();
				}
			} else {
				if ((inMemorySamples == null) || (inMemorySamples.isEmpty())) {
					throw new IOException("No sample is available.");
				}

				Sample sample = inMemorySamples.poll();
				sampleTime = sample.time;
				System.arraycopy(sample.values, 0, sampleValues, 0, sampleValues.length);
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
				throw new IllegalStateException("No sample is available.");
			}

			return sampleTime;
		}

		/**
		 * Returns item values in the current sample.
		 * 
		 * @return the map with item values.
		 * @throws RuntimeException
		 *             if no sample is available.
		 */
		public Map<String, Number> getValues() {
			ensureSample();

			Map<String, Number> result = new HashMap<String, Number>();
			for (int i = 0; i < items.length; i++) {
				result.put(items[i].name, decodeValue(items[i], sampleValues[i]));
			}

			return result;
		}

		/**
		 * Return item value in the current sample.
		 * 
		 * @param name
		 *            the name of the item.
		 * @return the value of the item in the current sample.
		 */
		public Number getValue(String name) {
			ensureSample();

			int idx = findItemIndex(name);
			return decodeValue(items[idx], sampleValues[idx]);
		}

		/**
		 * Returns whether item value in the current sample is null.
		 * 
		 * @param name
		 *            the name of the item.
		 * @return true, if the item value is null, false otherwise.
		 */
		public boolean isNullValue(String name) {
			ensureSample();

			int idx = findItemIndex(name);
			return sampleValues[idx] == NULL_VALUE;
		}

		/**
		 * Returns item value as a long value.
		 * 
		 * @param name
		 *            the name of the item.
		 * @return value of the item.
		 */
		public long getValueAsLong(String name) {
			ensureSample();

			int idx = findItemIndex(name);
			if (items[idx].decimals != 0) {
				throw new IllegalArgumentException("The item \"" + name + "\" is a double value.");
			}

			if (sampleValues[idx] == NULL_VALUE) {
				throw new NullPointerException("The value of item is null.");
			}

			return sampleValues[idx];
		}

		/**
		 * Returns item value as an integer value.
		 * 
		 * @param name
		 *            the name of the item.
		 * @return value of the item.
		 */
		public int getValueAsInt(String name) {
			return (int) getValueAsLong(name);
		}

		/**
		 * Returns item value as a double value.
		 * 
		 * @param name
		 *            the name of the item.
		 * @return value of the item.
		 */
		public double getValueAsDouble(String name) {
			ensureSample();

			int idx = findItemIndex(name);
			if (sampleValues[idx] == NULL_VALUE) {
				throw new NullPointerException("The value of item is null.");
			}

			if (items[idx].decimals == 0) {
				return sampleValues[idx];
			} else {
				return ((double) sampleValues[idx]) / items[idx].decimalsPower;
			}
		}

		/**
		 * Throws exception if no sample is loaded by the reader.
		 */
		private void ensureSample() throws IllegalStateException {
			if (sampleTime < 0) {
				throw new IllegalStateException("No sample is available.");
			}
		}

		/**
		 * Returns index of item with given name or throws exception if such an
		 * item does not exist.
		 * 
		 * @param name
		 *            the name of item.
		 * @return the index of item.
		 * @throws IllegalArgumentException
		 *             if item with required name does not exist.
		 */
		private int findItemIndex(String name) throws IllegalArgumentException {
			Integer index = itemIndices.get(name);
			if (index == null) {
				throw new IllegalArgumentException("The item \"" + name + "\" does not exist.");
			}

			return index;
		}

		/**
		 * Decodes raw value of an item in the current sample.
		 * 
		 * @param item
		 *            the description of the item.
		 * @param rawValue
		 *            the raw value.
		 * @return the decoded value.
		 */
		private Number decodeValue(Item item, long rawValue) {
			if (rawValue == NULL_VALUE) {
				return null;
			} else {
				if (item.decimals == 0) {
					return new Long(rawValue);
				} else {
					return new Double(((double) rawValue) / item.decimalsPower);
				}
			}
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}

			closeFile();

			if (inMemorySamples != null) {
				inMemoryReaders.remove(this);
				inMemorySamples = null;
			}

			closed = true;
		}

		/**
		 * Loads next sample from the storage file.
		 * 
		 * @return true, if the next sample has been fetched, false, if EOF is
		 *         reached.
		 */
		private boolean fetchNextSampleFromFile() throws IOException {
			try {
				int recordType = dataInput.readUnsignedByte();

				// skip index tables
				while (recordType == RecordType.INDEX_TABLE) {
					// skip the index table (the method skip of dataInput is not
					// used, since it does not provide required guarantees)
					int bytesToSkip = getRawIndexTableSizeInBytes() - 1;
					for (int i = 0; i < bytesToSkip; i++) {
						dataInput.readUnsignedByte();
					}

					recordType = dataInput.readUnsignedByte();
				}

				if (recordType == RecordType.DIF_SAMPLE) {
					if (nextSampleTime < 0) {
						throw new IOException("Malformed file (with respect to the expected format).");
					}

					nextSampleTime += readVLong(dataInput);
					for (int i = 0; i < nextSampleValues.length; i++) {
						long value = readVLong(dataInput);
						if ((nextSampleValues[i] != NULL_VALUE) && (value != NULL_VALUE)) {
							nextSampleValues[i] += value;
						} else {
							nextSampleValues[i] = value;
						}
					}
				} else if (recordType == RecordType.SAMPLE) {
					nextSampleTime = readVLong(dataInput);
					for (int i = 0; i < nextSampleValues.length; i++) {
						nextSampleValues[i] = readVLong(dataInput);
					}
				} else {
					throw new IOException("Malformed file (with respect to the expected format).");
				}

				return true;
			} catch (EOFException e) {
				return false;
			}
		}

		/**
		 * Silently closes resources.
		 */
		private void closeFile() {
			try {
				if (fileLock != null) {
					fileLock.release();
				}
			} catch (Exception ignore) {
				// silently ignore exceptions
			}

			try {
				if (storageFile != null) {
					storageFile.close();
				}
			} catch (Exception ignore) {
				// silently ignore exceptions
			}

			dataInput = null;
		}

		/**
		 * Activates access to in-memory samples.
		 */
		private void activateInMemorySamples() {
			closeFile();

			inMemorySamples = new LinkedList<>();
			for (Sample sample : unflushedSamples) {
				if (sample.time >= fromTime) {
					inMemorySamples.offer(sample);
				}
			}

			inMemoryReaders.add(this);
			nextSampleTime = -1;
		}
	}

	/**
	 * Storage file.
	 */
	private final File file;

	/**
	 * Last known file size.
	 */
	private long fileSize;

	/**
	 * Items forming samples in stored time series.
	 */
	private final Item[] items;

	/**
	 * Indices of items by name.
	 */
	private final Map<String, Integer> itemIndices = new HashMap<String, Integer>();

	/**
	 * Position of the first record (main index table).
	 */
	private final long dataOffset;

	/**
	 * Size of index table (maximal number of records).
	 */
	private final int indexTableSize;

	/**
	 * Minimal size of a continuous block of data records.
	 */
	private final int dataBlockSize;

	/**
	 * Main (master) index table.
	 */
	private IndexTable mainIndexTable;

	/**
	 * index table that contains offset of the trailing continuous data block.
	 */
	private IndexTable trailingIndexTable;

	/**
	 * Time of the last sample added to the storage.
	 */
	private long storageTime = -1;

	/**
	 * List of samples that are not stored in the associated storage file.
	 */
	private final List<Sample> unflushedSamples = new ArrayList<Sample>();

	/**
	 * List of active in-memory readers.
	 */
	private final List<SampleReader> inMemoryReaders = new ArrayList<>();

	/**
	 * The last stored sample or null, if no sample has been stored by this
	 * instance of the storage.
	 */
	private Sample lastStoredSample;

	/**
	 * True, if the "autoflush" feature is enabled, false otherwise.
	 */
	private boolean autoFlush = false;

	/**
	 * Time when the last flush of samples occurred.
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
	 * Constructs the storage and reads basic metadata from the file.
	 * 
	 * @param file
	 *            the storage file.
	 * 
	 * @throws IOException
	 *             if reading of storage file failed.
	 */
	public TimeSeriesStorage(File file) throws IOException {
		this.file = file;

		try (RandomAccessFile storageFile = new RandomAccessFile(file, "r")) {
			FileLock lock = storageFile.getChannel().tryLock(0, Long.MAX_VALUE, true);
			if (lock == null) {
				throw new IOException("File " + file.getAbsolutePath() + " is locked.");
			}

			try {
				// size of file in the moment of opening
				fileSize = storageFile.length();

				// create buffered input stream
				DataInputStream in = new DataInputStream(
						new BufferedInputStream(Channels.newInputStream(storageFile.getChannel())));

				// validate magic bytes (2B)
				for (int magicByte : MAGIC_BYTES) {
					if (magicByte != in.readUnsignedByte()) {
						throw new IOException("Invalid magic bytes.");
					}
				}

				// data offset (4B)
				dataOffset = in.readInt();
				// size of index tables (1B)
				indexTableSize = in.readUnsignedByte();
				// minimal size of continuous data block (2B)
				dataBlockSize = in.readUnsignedShort() * 100;

				// read structure of items and create map with item indices
				items = readItems(in);
				for (int i = 0; i < items.length; i++) {
					itemIndices.put(items[i].name, i);
				}
				if (itemIndices.size() != items.length) {
					throw new IOException("Invalid items of samples, duplicated names of items.");
				}

				// read the main index table
				mainIndexTable = readIndexTable(storageFile, dataOffset);

				// read index table with offset of trailing data block
				trailingIndexTable = findDirectIndexTableForTime(storageFile, Long.MAX_VALUE);

				// initializes the storage time
				initializeStorageTime(storageFile);
			} finally {
				lock.release();
			}
		}
	}

	/**
	 * Return names of items.
	 * 
	 * @return the array with item names.
	 */
	public String[] getItemNames() {
		String[] result = new String[items.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = items[i].name;
		}

		return result;
	}

	/**
	 * Returns whether auto-flush feature is enabled.
	 * 
	 * @return true, if the feature is enabled, false otherwise.
	 * @see TimeSeriesStorage#setAutoFlush(boolean)
	 */
	public boolean isAutoFlush() {
		return autoFlush;
	}

	/**
	 * Sets the auto-flush feature. If auto-flush is enabled, the method
	 * {@link TimeSeriesStorage#addSample(long, Map)} can automatically invoke
	 * the method {@link TimeSeriesStorage#flush()}.
	 * 
	 * @param autoFlush
	 *            true, to enable auto-flush, false to disable.
	 */
	public void setAutoFlush(boolean autoFlush) {
		this.autoFlush = autoFlush;
	}

	/**
	 * Returns the maximal number of unflushed samples, in the case when the
	 * auto-flush feature is enabled.
	 * 
	 * @return the maximal number of unflushed samples.
	 */
	public int getMaxUnflushedSamples() {
		return maxUnflushedSamples;
	}

	/**
	 * Sets the maximal number of unflushed samples, in the case when the
	 * auto-flush feature is enabled.
	 * 
	 * @param maxUnflushedSamples
	 *            the maximal number of unflushed samples.
	 */
	public void setMaxUnflushedSamples(int maxUnflushedSamples) {
		this.maxUnflushedSamples = Math.max(maxUnflushedSamples, 0);
	}

	/**
	 * Returns the maximal time between two flushes, in the case when the
	 * auto-flush feature is enabled.
	 * 
	 * @return the maximal time between two flushed in seconds.
	 */
	public long getMaxSecondsBetweenFlushes() {
		return maxSecondsBetweenFlushes;
	}

	/**
	 * Sets the maximal time between two flushes, in the case when the
	 * auto-flush feature is enabled.
	 * 
	 * @param maxSecondsBetweenFlushes
	 *            the maximal time between two flushed in seconds.
	 */
	public void setMaxSecondsBetweenFlushes(long maxSecondsBetweenFlushes) {
		this.maxSecondsBetweenFlushes = Math.max(maxSecondsBetweenFlushes, 0);
	}

	/**
	 * Returns file where data are stored.
	 * 
	 * @return the storage file.
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Returns an empty sample, i.e., with undefined values.
	 * 
	 * @return the empty sample.
	 */
	public Map<String, Number> createEmptySample() {
		Map<String, Number> result = new HashMap<>();
		for (Item item : items) {
			result.put(item.name, null);
		}

		return result;
	}

	/**
	 * Creates new reader that iterates stored samples originated after given
	 * time point.
	 * 
	 * @param fromTime
	 *            the minimal time of samples.
	 * @return the reader.
	 * @throws IOException
	 *             if reading from storage failed.
	 */
	public SampleReader createReader(long fromTime) throws IOException {
		return new SampleReader(fromTime);
	}

	/**
	 * Loads complete index structure and prints it.
	 * 
	 * @throws IOException
	 *             if reading from storage file failed.
	 */
	public void debugIndexStructure() throws IOException {
		try (RandomAccessFile storageFile = new RandomAccessFile(file, "r")) {
			FileLock fileLock = storageFile.getChannel().tryLock(0, Long.MAX_VALUE, true);
			if (fileLock == null) {
				throw new IOException("Storage file (" + file + ") is locked.");
			}

			try {
				if (fileSize != storageFile.length()) {
					throw new IOException("Concurrent modification of the storage file.");
				}
				loadIndexTableRecursively(storageFile, mainIndexTable);
			} finally {
				fileLock.release();
			}
		}

		System.out.println("Block size: " + dataBlockSize + " bytes");
		System.out.println("Size of index tables: " + indexTableSize + " records");
		System.out.println("Height of index tree: " + mainIndexTable.level);
		printIndexTableRecursively(mainIndexTable, 0);
	}

	/**
	 * Recursively loads all subtables of an index table.
	 * 
	 * @param storageFile
	 *            the storage file open for reading.
	 * @param indexTable
	 *            the index table
	 * @throws IOException
	 *             if reading failed.
	 */
	private void loadIndexTableRecursively(RandomAccessFile storageFile, IndexTable indexTable) throws IOException {
		if (indexTable.level == 0) {
			return;
		}

		for (int i = 0; i < indexTable.size; i++) {
			if (indexTable.subtables[i] == null) {
				indexTable.subtables[i] = readIndexTable(storageFile, indexTable.offsets[i]);
			}

			loadIndexTableRecursively(storageFile, indexTable.subtables[i]);
		}
	}

	/**
	 * Recursively print content of an index table.
	 * 
	 * @param table
	 *            the table to be printed.
	 * @param indentation
	 *            the indentation level.
	 */
	private void printIndexTableRecursively(IndexTable table, int indentation) {
		String indentationString = "";
		for (int i = 0; i < indentation; i++) {
			indentationString = indentationString + " ";
		}

		System.out.println(indentationString + "[" + table.level + " (" + table.size + "): " + table.offset + "]");
		for (int i = 0; i < table.size; i++) {
			System.out.println(indentationString + "  |" + table.startTimes[i] + " -> " + table.offsets[i]);
			if (table.subtables[i] != null) {
				printIndexTableRecursively(table.subtables[i], indentation + 4);
			}
		}
	}

	/**
	 * Returns the time of the latest sample.
	 * 
	 * @return the time of the latest sample or a negative number if the storage
	 *         does not contain any sample.
	 */
	public long getTime() {
		return storageTime;
	}

	/**
	 * Adds new sample to storage and flushes unflushed samples if necessary.
	 * 
	 * @param time
	 *            the time of sample (arbitrary increasing non-negative value)
	 * @param values
	 *            the map with values of items forming the sample.
	 * @throws IOException
	 *             if writing the sample failed.
	 * @throws IllegalArgumentException
	 *             if the sample is not complete (a missing value) or time of
	 *             sample is invalid.
	 */
	public void addSample(long time, Map<String, ? extends Number> values)
			throws IOException, IllegalArgumentException {
		// check time of sample
		if (time <= storageTime) {
			throw new IllegalArgumentException(
					"Time of sample is equal or smaller than time of the latest sample in the storage.");
		}

		// create array with raw values (index 0 stores time)
		long[] rawValues = new long[items.length];
		for (int i = 0; i < items.length; i++) {
			Number value = values.get(items[i].name);
			if (value == null) {
				if (!values.containsKey(items[i].name)) {
					throw new IllegalArgumentException("Value of the item \"" + items[i].name + "\" is not provided.");
				}
			}

			if (value != null) {
				rawValues[i] = Math.round(value.doubleValue() * items[i].decimalsPower);
				// special handling for MIN_VALUE which is equal to
				// NULL_VALUE
				if (rawValues[i] == NULL_VALUE) {
					rawValues[i] = Long.MIN_VALUE + 1;
				}
			} else {
				rawValues[i] = NULL_VALUE;
			}
		}

		// add sample to storage
		Sample sample = new Sample(time, rawValues);
		unflushedSamples.add(sample);
		storageTime = time;

		// add sample to in-memory readers
		if (!inMemoryReaders.isEmpty()) {
			for (SampleReader reader : inMemoryReaders) {
				if (sample.time >= reader.fromTime) {
					reader.inMemorySamples.offer(sample);
				}
			}
		}

		if (autoFlush) {
			// flush samples to storage file (if required)
			long secondsFromLastFlush = (MonotonicClock.INSTANCE.currentTimeMillis() - timeOfLastFlush) / 1000;
			if ((unflushedSamples.size() > maxUnflushedSamples) || (secondsFromLastFlush < 0)
					|| (secondsFromLastFlush > maxSecondsBetweenFlushes)) {
				flush();
			}
		}
	}

	/**
	 * Writes all unflushed samples to the storage.
	 * 
	 * @throws IOException
	 *             if the operation failed.
	 */
	public void flush() throws IOException {
		// store time of the last flush
		timeOfLastFlush = MonotonicClock.INSTANCE.currentTimeMillis();

		// check whether there is a sample to be flushed
		if (unflushedSamples.isEmpty()) {
			return;
		}

		try (RandomAccessFile storageFile = new RandomAccessFile(file, "rw")) {
			FileLock lock = storageFile.getChannel().tryLock(0, Long.MAX_VALUE, false);
			if (lock == null) {
				throw new IOException("File " + file.getAbsolutePath() + " is locked.");
			}

			try {
				if (storageFile.length() != fileSize) {
					throw new IOException("Concurrent modification of the storage file.");
				}

				DataOutputStream out = null;
				for (Sample sample : unflushedSamples) {
					// determine whether new block is required
					long trailerBlockOffset = trailingIndexTable.offsets[trailingIndexTable.size - 1];
					boolean newBlockRequired = (storageFile.length() - trailerBlockOffset > dataBlockSize);

					// create new block (if required)
					if (newBlockRequired) {
						// close output
						if (out != null) {
							out.flush();
							out = null;
						}

						// create new block
						createNewBlock(storageFile, sample.time);

						// reset sample history
						lastStoredSample = null;
					}

					// create output (if not yet created)
					if (out == null) {
						storageFile.seek(storageFile.length());
						out = new DataOutputStream(
								new BufferedOutputStream(Channels.newOutputStream(storageFile.getChannel())));
					}

					// write sample
					writeSample(out, sample);
				}

				if (out != null) {
					out.flush();
					out = null;
				}

				unflushedSamples.clear();
				fileSize = storageFile.length();
			} finally {
				lock.release();
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
		// serialize sample
		if (lastStoredSample == null) {
			out.writeByte(RecordType.SAMPLE);
			writeVLong(out, sample.time);
			for (long value : sample.values) {
				writeVLong(out, value);
			}
		} else {
			out.writeByte(RecordType.DIF_SAMPLE);
			writeVLong(out, sample.time - lastStoredSample.time);
			for (int i = 0; i < sample.values.length; i++) {
				if ((sample.values[i] != NULL_VALUE) && (lastStoredSample.values[i] != NULL_VALUE)) {
					writeVLong(out, sample.values[i] - lastStoredSample.values[i]);
				} else {
					writeVLong(out, sample.values[i]);
				}
			}
		}

		// store reference to last stored sample for differential encoding
		lastStoredSample = sample;
	}

	/**
	 * Creates new block at the end of the file for samples created at given
	 * time or later.
	 * 
	 * @param storageFile
	 *            the storage file open for writing.
	 * @param startTime
	 *            the start time of the block.
	 * 
	 * @throws IOException
	 *             if the operation failed.
	 */
	private void createNewBlock(RandomAccessFile storageFile, long startTime) throws IOException {
		// if the trailing index table is not full, we just append new index to
		// data block
		if (!trailingIndexTable.isFull()) {
			trailingIndexTable.appendIndex(startTime, storageFile.length());
			trailingIndexTable.write(storageFile);
			return;
		}

		IndexTable[] trailingBranch = createFullTrailingBranch();

		// if all index tables are full, it is necessary to create new main
		// index
		// table with greater level
		if (trailingBranch == null) {
			increaseStorageIndexLevel(storageFile);
			trailingBranch = createFullTrailingBranch();
		}

		// create new index tables for full levels
		long startOffset = storageFile.length();
		trailingBranch[0].appendIndex(startTime, startOffset);
		for (int i = 1; i < trailingBranch.length; i++) {
			trailingBranch[i] = new IndexTable(startOffset, trailingBranch[i - 1].level - 1);
			startOffset += getRawIndexTableSizeInBytes();
			trailingBranch[i].appendIndex(startTime, startOffset);
		}

		// link index tables
		for (int i = 0; i < trailingBranch.length - 1; i++) {
			trailingBranch[i].subtables[trailingBranch[i].size - 1] = trailingBranch[i + 1];
		}

		// write index tables
		for (int i = 0; i < trailingBranch.length; i++) {
			trailingBranch[i].write(storageFile);
		}

		trailingIndexTable = trailingBranch[trailingBranch.length - 1];
	}

	/**
	 * Returns the longest suffix of path from main index table to the trailing
	 * index table formed by full index tables. The path also includes the
	 * predecessor which is not full (if exists).
	 * 
	 * @return the path.
	 */
	private IndexTable[] createFullTrailingBranch() {
		IndexTable[] trailingBranch = new IndexTable[mainIndexTable.level + 1];
		trailingBranch[0] = mainIndexTable;
		for (int i = 1; i < trailingBranch.length; i++) {
			IndexTable previous = trailingBranch[i - 1];
			trailingBranch[i] = previous.subtables[previous.size - 1];
		}

		int pathStartIdx = -1;
		for (int i = 0; i < trailingBranch.length; i++) {
			if (!trailingBranch[i].isFull()) {
				pathStartIdx = i;
			}
		}

		if (pathStartIdx >= 0) {
			return Arrays.copyOfRange(trailingBranch, pathStartIdx, trailingBranch.length);
		} else {
			return null;
		}
	}

	/**
	 * Increases level of the main index table.
	 * 
	 * @param storageFile
	 *            the storage file open for writing.
	 */
	private void increaseStorageIndexLevel(RandomAccessFile storageFile) throws IOException {
		IndexTable shiftedMainTable = mainIndexTable.cloneWithNewOffset(storageFile.length());
		shiftedMainTable.write(storageFile);

		IndexTable newMainTable = new IndexTable(mainIndexTable.offset, mainIndexTable.level + 1);
		newMainTable.appendIndex(mainIndexTable.startTimes[0], shiftedMainTable.offset);
		newMainTable.write(storageFile);

		mainIndexTable = newMainTable;
		trailingIndexTable = findDirectIndexTableForTime(storageFile, Long.MAX_VALUE);
	}

	/**
	 * Reads index (index) table.
	 * 
	 * @param storageFile
	 *            the storage file open for reading.
	 * @param offset
	 *            the file offset of index table to be read.
	 * 
	 * @throws IOException
	 *             if read failed.
	 */
	private IndexTable readIndexTable(RandomAccessFile storageFile, long offset) throws IOException {
		if (offset <= 0) {
			throw new IOException("Malformed file (with respect to the expected format).");
		}

		storageFile.seek(offset);

		// initialized buffered reading
		DataInput in = new DataInputStream(new BufferedInputStream(Channels.newInputStream(storageFile.getChannel())));

		// heading - type of record
		int headingByte = in.readUnsignedByte();
		if (headingByte != RecordType.INDEX_TABLE) {
			throw new IOException("Malformed file (with respect to the expected format).");
		}

		// level of index table
		int level = in.readByte();
		if (level < 0) {
			throw new IOException("Malformed file (with respect to the expected format).");
		}

		// number of valid records
		int recordCount = in.readUnsignedByte();
		if (recordCount > indexTableSize) {
			throw new IOException("Malformed file (with respect to the expected format).");
		}

		IndexTable result = new IndexTable(offset, level);
		for (int i = 0; i < recordCount; i++) {
			long startTime = in.readLong();
			long indexOffset = in.readLong();
			if ((startTime < 0) || (indexOffset <= 0)) {
				throw new IOException("Malformed file (with respect to the expected format).");
			}

			result.appendIndex(startTime, indexOffset);
		}

		return result;
	}

	/**
	 * Returns the size of encoded index table in bytes.
	 * 
	 * @return the size of encoded index table in bytes.
	 */
	private int getRawIndexTableSizeInBytes() {
		return 3 + indexTableSize * 2 * 8;
	}

	/**
	 * Returns (and eventually loads) index table that contains offset of the
	 * first data block that contains samples originated after a given time.
	 * 
	 * @param storageFile
	 *            the storage file open for reading.
	 * @param time
	 *            the time.
	 * @return the index table with level 0.
	 * 
	 * @throws IOException
	 *             if reading of the file failed.
	 */
	private IndexTable findDirectIndexTableForTime(RandomAccessFile storageFile, long time) throws IOException {
		IndexTable indexTable = mainIndexTable;
		while (indexTable.level > 0) {
			int bestIndex = findLatestIndexForTime(indexTable, time);
			if (indexTable.subtables[bestIndex] == null) {
				indexTable.subtables[bestIndex] = readIndexTable(storageFile, indexTable.offsets[bestIndex]);
			}

			if (indexTable.subtables[bestIndex].level >= indexTable.level) {
				throw new IOException("Malformed file (invalid structure of index tables).");
			}

			indexTable = indexTable.subtables[bestIndex];
		}

		return indexTable;
	}

	/**
	 * Returns the largest index in the index table that contains samples
	 * originated after a given time.
	 * 
	 * @param indexTable
	 *            the search table.
	 * @param time
	 *            the time.
	 * @return the index.
	 */
	private int findLatestIndexForTime(IndexTable indexTable, long time) {
		if (time < indexTable.startTimes[0]) {
			return 0;
		}

		for (int i = indexTable.size - 1; i >= 0; i--) {
			if (time >= indexTable.startTimes[i]) {
				return i;
			}
		}

		return 0;
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
		int itemsCount = in.readUnsignedShort();
		if (itemsCount > Short.MAX_VALUE) {
			throw new IOException("Malformed file (with respect to the expected format).");
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
	 * Initializes the storage time.
	 * 
	 * @param storageFile
	 *            the storage file open for reading.
	 */
	private void initializeStorageTime(RandomAccessFile storageFile) throws IOException {
		long trailerBlockOffset = trailingIndexTable.offsets[trailingIndexTable.size - 1];

		storageFile.seek(trailerBlockOffset);
		DataInputStream dataInput = new DataInputStream(
				new BufferedInputStream(Channels.newInputStream(storageFile.getChannel())));

		// iterate records in the trailing block
		long time = -1;
		int recordType = 0;
		while (true) {
			try {
				recordType = dataInput.readUnsignedByte();
			} catch (EOFException e) {
				storageTime = time;
				return;
			}

			if (recordType == RecordType.INDEX_TABLE) {
				// skip the index table (the method skip of dataInput is not
				// used, since it does not provide required guarantees)
				int bytesToSkip = getRawIndexTableSizeInBytes() - 1;
				for (int i = 0; i < bytesToSkip; i++) {
					dataInput.readUnsignedByte();
				}
			} else if (recordType == RecordType.DIF_SAMPLE) {
				if (time < 0) {
					throw new IOException("Malformed file (with respect to the expected format).");
				}
				time += readVLong(dataInput);

				// skip item values
				for (int i = 0; i < items.length; i++) {
					readVLong(dataInput);
				}
			} else if (recordType == RecordType.SAMPLE) {
				time = readVLong(dataInput);

				// skip item values
				for (int i = 0; i < items.length; i++) {
					readVLong(dataInput);
				}
			} else {
				throw new IOException("Malformed file (with respect to the expected format).");
			}
		}
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
	 * @param blockSize
	 *            the minimal length of a continuous data block.
	 * @throws IOException
	 *             if creation of storage failed.
	 */
	public static void create(File file, Map<String, Integer> items, long blockSize) throws IOException {
		blockSize = Math.max(blockSize / 100, 1);
		blockSize = Math.min(blockSize, Short.MAX_VALUE);
		int indexTableSize = INDEX_TABLE_SIZE;

		try (RandomAccessFile storageFile = new RandomAccessFile(file, "rw")) {
			FileLock lock = storageFile.getChannel().tryLock(0, Long.MAX_VALUE, false);
			if (lock == null) {
				throw new IOException("File " + file.getAbsolutePath() + " is locked.");
			}

			try {
				if (storageFile.length() != 0) {
					throw new IOException("The storage file " + file.getAbsolutePath() + " already exists.");
				}

				DataOutputStream out = new DataOutputStream(
						new BufferedOutputStream(Channels.newOutputStream(storageFile.getChannel())));

				// write magic bytes (2B)
				for (int magicByte : MAGIC_BYTES) {
					out.writeByte(magicByte);
				}

				// write placeholder for data offset (4B)
				out.writeInt(0);
				// write size of index tables (1B)
				out.writeByte(indexTableSize);
				// write minimal size of continuous data block in multiplies of
				// 100B (2B)
				out.writeShort((int) blockSize);

				// write description of items forming a sample
				out.writeShort(items.size());
				for (Map.Entry<String, Integer> entry : items.entrySet()) {
					out.writeUTF(entry.getKey());
					int decimals = entry.getValue();
					decimals = Math.min(Math.max(decimals, 0), MAX_DECIMALS);
					out.writeByte(decimals);
				}

				int dataOffset = out.size();

				// write initial main index table
				out.writeByte(RecordType.INDEX_TABLE);
				out.writeByte(0); // level
				out.writeByte(1); // number of valid records

				// the first (initial) index
				out.writeLong(0); // time
				out.writeLong(dataOffset + 3 + indexTableSize * (2 * 8)); // offset

				// the remaining index records
				for (int i = 1; i < indexTableSize; i++) {
					out.writeLong(0);
					out.writeLong(0);
				}

				// finalize and flush data
				out.flush();

				// update data offset
				storageFile.seek(MAGIC_BYTES.length);
				storageFile.writeInt(dataOffset);
			} finally {
				lock.release();
			}
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
