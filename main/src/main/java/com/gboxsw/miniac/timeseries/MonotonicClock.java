package com.gboxsw.miniac.timeseries;

/**
 * Implementation of a monotonic clock.
 */
final class MonotonicClock {

	/**
	 * Instance of a monotonic clock.
	 */
	public static final MonotonicClock INSTANCE = new MonotonicClock();

	/**
	 * Start-time of monotonic clock.
	 */
	private final long nanoTime0;

	/**
	 * Construct a new monotonic clock.
	 */
	public MonotonicClock() {
		nanoTime0 = System.nanoTime();
	}

	/**
	 * Returns current time in milliseconds.
	 * 
	 * @return the difference, measured in milliseconds, between the current
	 *         time and time when the clock was created.
	 */
	public long currentTimeMillis() {
		return (System.nanoTime() - nanoTime0) / 1_000_000l;
	}
}
