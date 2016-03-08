package com.jesseanderson.monkeys;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.hash.Hash;

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
 * Takes the possible false positives and checks them against the actual works to see if they are there
 */
public class MonkeyReducer extends MapReduceBase implements
		Reducer<LongWritable, StringArrayWritable, LongWritable, StringArrayWritable> {
	// Bloom Filter variables (The Bloom Filter used to be instantiated in this class)
	/** The vector size for the Bloom Filter */
	public static final int vectorSize = 100000000;
	/** The number of hashes for the Bloom Filter */
	public static final int nbHash = 6;// 500000;
	/** The type of hashing to use for the Bloom Filter */
	public static final int hashType = Hash.MURMUR_HASH;
	/** The prefix to use when writing out the Bloom Filter */
	public static final String prefix = "shakespeare1";
	/** The path to use when reading and writing out the Bloom Filter */
	public static final Path inputFile = new Path(MonkeyUtils.prefix + "pg100.txt");
	
	public void reduce(LongWritable key, Iterator<StringArrayWritable> values,
			OutputCollector<LongWritable, StringArrayWritable> output, Reporter reporter)
			throws IOException {
		// Go through every StringArrayWritable created by the mapper to see which ones are actually in the works
		while (values.hasNext()) {
			int passedMembership = 0;
			
			StringArrayWritable stringArrayWritable = values.next();
			Writable[] randomStrings = stringArrayWritable.get();
			
			for (int i = 0; i < randomStrings.length; i++) {
				String randomString = ((Text) randomStrings[i]).toString();
				
				String input = MonkeyUtils.getText(inputFile, MonkeyUtils.QUOTE_SIZE,
						prefix, vectorSize, nbHash, hashType);
				
				passedMembership++;
				
				// TODO: This part is inefficient and should use Lucene or something faster than indexOf
				// See if the random string is actually in the work
				if (input.contains(randomString)) {
					MonkeyUtils.logger.error("Found one \"" + randomString + "\" in " + key);
					System.err.println("Found one \"" + randomString + "\" in " + key);

					StringArrayWritable arrayWritable = new StringArrayWritable();
					arrayWritable.set(new Text[] {new Text(randomString)});
					
					output.collect(key, arrayWritable);
				}
			}
			
			//System.out.println("Passed membership: " + passedMembership);
		}
	}
}
