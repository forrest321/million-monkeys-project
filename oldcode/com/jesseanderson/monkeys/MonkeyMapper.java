package com.jesseanderson.monkeys;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

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
 * Performs the random string generation and run against a Bloom Filter to see if the
 * random string is found in the works.
 */
public class MonkeyMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, LongWritable, StringArrayWritable> {
	/** The current sub-iteration of the mapper */
	public static AtomicLong iteration = new AtomicLong();
	/** The number of iterations to run in a sub-iteration */
	// TODO: This should be a parameter
	public static final long MAP_ITERATION_SIZE = 1000000;
	/** A thread safe reference to the bloom filter so it is only loaded once */
	private static AtomicReference<BloomFilter> bloomFilter = new AtomicReference<BloomFilter>();

	public void map(LongWritable key, Text value,
			OutputCollector<LongWritable, StringArrayWritable> output, Reporter reporter)
			throws IOException {
		long iterationLocal = iteration.incrementAndGet();
		long groupSize = MAP_ITERATION_SIZE * iterationLocal;
		
		ArrayList<Text> outputArray = new ArrayList<Text>();
		
		BloomFilter localBloomFilter;
		
		if ((localBloomFilter = bloomFilter.get()) == null) {
			// Hasn't been initialized yet, load it
			localBloomFilter = MonkeyUtils.getBloomFilter(MonkeyReducer.inputFile, MonkeyUtils.QUOTE_SIZE, 
					MonkeyReducer.prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);
			
			bloomFilter.set(localBloomFilter);
		}
		
		// Generate the random output
		byte[] randomBytes = new byte[MonkeyUtils.QUOTE_SIZE];
		
		for (int i = 0; i < MAP_ITERATION_SIZE; i++) {
			MonkeyUtils.getRandomBytes(randomBytes);
			
			// See if the Bloom Filter say it might be there
			if (localBloomFilter.membershipTest(new Key(randomBytes))) {
				// It might be there, send on to reducer for double check
				String randomString = MonkeyUtils.getStringForBytes(randomBytes);
				
				// System.out.println("Membership test passed for \"" + randomString + "\"");
				
				outputArray.add(new Text(randomString));
			}
		}
		
		// Create StringArrayWritable containing all of the random words found
		StringArrayWritable stringArrayWritable = new StringArrayWritable();
		stringArrayWritable.set(outputArray.toArray(new Text[outputArray.size()]));
		
		output.collect(new LongWritable(groupSize), stringArrayWritable);
		
		System.out.println("Iteration:" + iterationLocal + " groupSize:" + groupSize);
	}
}
