package com.jesseanderson.monkeys;

/*
 * Copyright 2011 Jesse Anderson
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * A very basic Stopwatch class for doing timings
 * @author jesse
 *
 */
public class Stopwatch {
	/** The begin time in ms */
	long begin;
	/** The end time in ms */
	long end;
	
	/**
	 * Starts the stopwatch
	 */
	public void start() {
		begin = System.currentTimeMillis();
	}
	
	/**
	 * Stops the stopwatch
	 * @return The current Stopwatch object
	 */
	public Stopwatch stop() {
		end = System.currentTimeMillis();
		
		return this;
	}
	
	/**
	 * Prints out the time and name of the Stopwatch
	 * @param name The display name of the Stopwatch
	 */
	public void display(String name) {
		System.out.println(name + " " + time());
	}
	
	/**
	 * Calculates the elapsed time
	 * @return The time in ms
	 */
	public long time() {
		return end - begin;
	}
}
