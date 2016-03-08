package com.jesseanderson.monkeys;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.*;
import static org.junit.Assert.*;

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

public class MonkeyMapTests {
	@BeforeClass
	public static void setup() {
		MonkeyUtils.prefix = "";
	}
	
	/**
	 * Tests that every character group can be found in Shakespeare
	 * then changes the character group slightly and test that the
	 * character group can not be found.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testBloom() throws Exception {
		String prefix = "shakespeare1";
		Path inputPath = new Path("pg100.txt");

		BloomFilter bloomFilter = MonkeyUtils.getBloomFilter(inputPath,
				MonkeyUtils.QUOTE_SIZE, prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);

		String input = MonkeyUtils.getText(inputPath, MonkeyUtils.QUOTE_SIZE,
				prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);
		String[] brokenUp = MonkeyUtils.breakUp(input, MonkeyUtils.QUOTE_SIZE);

		boolean contains = bloomFilter.membershipTest(new Key(input.substring(
				0, MonkeyUtils.QUOTE_SIZE).getBytes()));

		assertTrue("Beginning not found", contains);

		for (String piece : brokenUp) {
			contains = bloomFilter.membershipTest(new Key(piece.getBytes()));

			assertTrue("In array not found " + piece, contains);
		}

		int found = 0;

		for (String piece : brokenUp) {
			String changed = "@" + piece.substring(1);
			contains = bloomFilter.membershipTest(new Key(changed.getBytes()));

			if (contains) {
				found++;
			}
			// assertFalse("Found changed " + piece, contains);
		}

		MonkeyUtils.logger
				.info("Found:" + found + " Out of:" + brokenUp.length);
	}
	
	/**
	 * Checks the speed difference between a Bloom Filter and an indexOf
	 * @throws Exception
	 */
	@Test
	public void testBloomAndContainsSpeed() throws Exception {
		String prefix = "shakespeare1";
		Path inputPath = new Path("pg100.txt");

		BloomFilter bloomFilter = MonkeyUtils.getBloomFilter(inputPath,
				MonkeyUtils.QUOTE_SIZE, prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);

		String input = MonkeyUtils.getText(inputPath, MonkeyUtils.QUOTE_SIZE,
				prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);
		String[] brokenUp = MonkeyUtils.breakUp(input, MonkeyUtils.QUOTE_SIZE);

		boolean contains = bloomFilter.membershipTest(new Key(input.substring(
				0, MonkeyUtils.QUOTE_SIZE).getBytes()));

		assertTrue("Beginning not found", contains);

		int totalTimes = 1;
		
		long[] bloomTimes = new long[totalTimes];
		long[] indexTimes = new long[totalTimes];
		
		for (int i = 0; i < totalTimes; i++) {
			// Do Bloom Filter timing
			Stopwatch bloomTime = new Stopwatch();
			
			bloomTime.start();
			for (String piece : brokenUp) {
				contains = bloomFilter.membershipTest(new Key(piece.getBytes()));
			}
			bloomTime.stop();
			
			bloomTimes[i] = bloomTime.time();
			
			// Do indexOf timing
			Stopwatch indexTime = new Stopwatch();
			
			indexTime.start();
			for (String piece : brokenUp) {
				input.indexOf(piece);
			}
			indexTime.stop();
			
			indexTimes[i] = indexTime.time();
		}
		
		long totalBloomTime = 0;
		long totalIndexTime = 0;
		
		for (int i = 0; i < totalTimes; i++) {
			totalBloomTime += bloomTimes[i];
			
			System.out.println("Bloom " + i + " " + bloomTimes[i]);
		}
		
		System.out.println("Bloom Average " + (totalBloomTime / totalTimes));
		
		for (int i = 0; i < totalTimes; i++) {
			totalIndexTime += indexTimes[i];
			
			System.out.println("Index " + i + " " + indexTimes[i]);
		}
		
		System.out.println("Index Average " + (totalIndexTime / totalTimes));
	}
	
	/**
	 * Figures out the number of unique character groups in the work
	 * @throws Exception
	 */
	@Test
	public void testUniqueGroups() throws Exception {
		String prefix = "test1";
		Path inputPath = new Path("pg100.txt");

		String input = MonkeyUtils.getText(inputPath, MonkeyUtils.QUOTE_SIZE,
				prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);
		String[] brokenUp = MonkeyUtils.breakUp(input, MonkeyUtils.QUOTE_SIZE);

		HashSet<String> uniqueGroups = new HashSet<String>();
		
		for (String string : brokenUp) {
			uniqueGroups.add(string);
		}

		MonkeyUtils.logger
				.info("Unique Groups:" + uniqueGroups.size() + " Out of:" + brokenUp.length);
	}

	/**
	 * Checks the Bloom Filter speed with random character groups
	 * @throws Exception
	 */
	@Test
	public void bloomSpeed() throws Exception {
		String prefix = "shakespeare1";
		Path inputPath = new Path("pg100.txt");

		BloomFilter bloomFilter = MonkeyUtils.getBloomFilter(inputPath,
				MonkeyUtils.QUOTE_SIZE, prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);

		int found = 0;
		long bloomPositives = 0;
		
		byte[] randomBytes = new byte[MonkeyUtils.QUOTE_SIZE];

		for (int i = 0; i < 10000000; i++) {
			MonkeyUtils.getRandomBytes(randomBytes);

			if (bloomFilter.membershipTest(new Key(randomBytes))) {
				bloomPositives++;

				String randomString = MonkeyUtils.getStringForBytes(randomBytes);
				String input = MonkeyUtils.getText(inputPath, MonkeyUtils.QUOTE_SIZE,
						prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);
				
				if (input.contains(randomString)) {
					found++;

					MonkeyUtils.logger.error("Found one \"" + randomString
							+ "\"");
				}
			}
		}
		
		System.out.println("Found: " + found);
	}

	/**
	 * Checks the indexOf speed with random character groups
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void indexOfSpeed() throws Exception {
		String prefix = "shakespeare1";
		Path inputPath = new Path("pg100.txt");

		MonkeyUtils.getBloomFilter(inputPath, MonkeyUtils.QUOTE_SIZE, prefix,
				MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);

		String input = MonkeyUtils.getText(inputPath, MonkeyUtils.QUOTE_SIZE,
				prefix, MonkeyReducer.vectorSize, MonkeyReducer.nbHash, MonkeyReducer.hashType);

		byte[] randomBytes = new byte[MonkeyUtils.QUOTE_SIZE];
		
		for (int i = 0; i < 10000; i++) {
			MonkeyUtils.getRandomBytes(randomBytes);
			String randomString = MonkeyUtils.getStringForBytes(randomBytes);
			input.indexOf(randomString);
		}
	}
	
	/**
	 * Verifies that the random output is only a-z 
	 */
	@Test
	public void allLettersFound() {
		boolean[] letters = new boolean[26];
		
		boolean allFound = true;
		
		Pattern pattern = Pattern.compile("[a-z]*");
		
		for (int i = 0; i < 10000; i++) {
			allFound = true;
			
			byte[] randomBytes = new byte[MonkeyUtils.QUOTE_SIZE];
			MonkeyUtils.getRandomBytes(randomBytes);
			String randomString = MonkeyUtils.getStringForBytes(randomBytes);
			
			assertTrue("Pattern not found.  String was \"" + randomString + "\"", pattern.matcher(randomString).matches());
			
			char[] characters = randomString.toCharArray();
			
			for (char character : characters) {
				letters[character - 97] = true;
			}
			
			for (boolean found : letters) {
				if (found == false) {
					allFound = false;
					
					break;
				}
			}
			
			if (allFound == true) {
				break;
			}
		}
		
		assertTrue("Could not find all letters", allFound == true);
	}

	/**
	 * Runs a cursory test on the mapper
	 * @throws IOException
	 */
	@Test
	public void checkMonkeyMapper() throws IOException {
		MonkeyMapper mapper = new MonkeyMapper();

		OutputCollector<LongWritable, StringArrayWritable> output = mock(OutputCollector.class);
		mapper.map(null, null, output, null);
		verify(output, atLeastOnce()).collect(any(LongWritable.class),
				any(StringArrayWritable.class));
	}

	/**
	 * Tests the reducer to verify only good data is found
	 * @throws IOException
	 */
	@Test
	public void checkMonkeyReducer() throws IOException {
		MonkeyReducer reducer = new MonkeyReducer();

		LongWritable key = new LongWritable(1);
		
		StringArrayWritable stringArrayWritable = new StringArrayWritable();
		
		String findStringBase = "akingbreastwhopleadforlo";
		String findString = findStringBase.substring(0, MonkeyUtils.QUOTE_SIZE);
		String findStringOther = findStringBase.substring(0, MonkeyUtils.QUOTE_SIZE - 1);
		
		Text[] texts = {
				new Text(findString),
				new Text(findStringOther + "1"),
				new Text(findStringOther + "2"),
				new Text(findStringOther + "3"),
				new Text(findStringOther + "4"),
				new Text(findStringOther + "5")
		};
		
		byte[] randomBytes = new byte[MonkeyUtils.QUOTE_SIZE];
		MonkeyUtils.getRandomBytes(randomBytes);
		String randomString = MonkeyUtils.getStringForBytes(randomBytes);
		
		stringArrayWritable.set(texts);
		ArrayList<StringArrayWritable> arrayList = new ArrayList<StringArrayWritable>();
		arrayList.add(stringArrayWritable);
		
		OutputCollector<LongWritable, StringArrayWritable> output = mock(OutputCollector.class);

		reducer.reduce(key, arrayList.iterator(), output, (Reporter) null);

		StringArrayWritable arrayWritable = new StringArrayWritable();
		arrayWritable.set(new Text[] {new Text(findString)});
		
		verify(output).collect(key, arrayWritable);
	}

	/**
	 * Runs a full test of the job by creating a driver
	 * @throws Exception
	 */
	@Test
	public void checkDriver() throws Exception {
		JobConf conf = new JobConf();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");

		Path input = new Path("input.txt");
		Path output = new Path("output");
		Path stopFile = new Path("stop.txt");

		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true); // delete old output
		
		// Create stop file to keep from iterating
		fs.create(stopFile);

		MonkeyDriver driver = new MonkeyDriver();
		driver.setConf(conf);

		int exitCode = driver.run(new String[] { input.toString(),
				output.toString() });
		assertEquals(exitCode, 0);

		//checkOutput(conf, output);
		
		// Delete stop file for next time
		fs.delete(stopFile, true);
	}
	
	// TODO: Write artificially completing test
}
