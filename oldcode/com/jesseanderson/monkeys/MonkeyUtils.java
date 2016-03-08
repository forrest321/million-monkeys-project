package com.jesseanderson.monkeys;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.log4j.Logger;

import ec.util.MersenneTwister;

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
 * A bunch of utility functions to aid in reading files, etc.  Most of the memory caching
 * only helped out the unit tests.
 */
public class MonkeyUtils {
	static {
		 URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	    
	public static final Logger logger = Logger.getLogger(MonkeyUtils.class);
	
	/** The size of the character group to use during processing.  This could be a parameter but I prefer a constant for now */
	public static int QUOTE_SIZE = 9;
	/** Static instance of the PRNG for the random character generation */
	private static MersenneTwister random = new MersenneTwister();
	/** Memory cache for Bloom Filter.  Only helps out in unit tests */
	private static HashMap<String, BloomFilter> paramemetersToBloomFilter = new HashMap<String, BloomFilter>();
	/** Memory cache for story text.  Only helps out in unit tests */
	private static HashMap<String, StringBuilder> pathToFileContents = new HashMap<String, StringBuilder>();
	/** Prefix.  Used when running on Amazon EC2 */
	public static String prefix = "";

	/** The number of letter in the alphabet */
	private static final byte LETTERS = 26;
	/** The offset in ACSII to get to the lowercase letters */
	private static final byte ASCII_OFFSET = 97;
		
	public static String getText(Path path, int quoteSize,
			String prefix, int vectorSize, int nbHash, int hashType) throws IOException {
		return loadFile(path).toString();
	}

	public static BloomFilter getBloomFilter(Path path, int quoteSize,
			String prefix, int vectorSize, int nbHash, int hashType)
			throws IOException {
		BloomFilter bloomFilter = MonkeyUtils.loadBloomFilter(prefix,
				vectorSize, nbHash, hashType);

		String bloomName = getBloomName(prefix, vectorSize, nbHash, hashType);
		
		if (bloomFilter == null) {
			System.out.println("Loading bloom filter");
			// Bloom not loaded already, load it
			StringBuilder builder = loadFile(path);
			
			bloomFilter = new BloomFilter(vectorSize, nbHash, hashType);

			logger.info("Breaking up file");
			MonkeyUtils.breakUp(bloomFilter, builder, quoteSize);
			logger.info("Broken up file");

			// Save bloom out
			MonkeyUtils.saveBloomFilter(bloomFilter, prefix, vectorSize,
					nbHash, hashType);

			
			paramemetersToBloomFilter.put(bloomName, bloomFilter);
		} else {
			//System.out.println("Loading bloom filter from memory");
			StringBuilder builder = loadFile(path);
			
			paramemetersToBloomFilter.put(bloomName, bloomFilter);
		}

		return bloomFilter;
	}

	private static StringBuilder loadFile(Path path) throws IOException {
		// See if already in memory
		StringBuilder builder = pathToFileContents.get(path.toString());
		
		if (builder != null) {
			return builder;
		}
		
		logger.info("Loading file " + path.toString());
		System.out.println("Loading file " + path.toString());

		builder = readEntireFile(path);
		
		pathToFileContents.put(path.toString(), builder);

		logger.info("Finished loading file " + path.toString() + " size:" + builder.length());
		
		return builder;
	}

	public static StringBuilder readEntireFile(Path path) throws IOException {
		StringBuilder builder;
		FileSystem fileSystem = getFilesystem();
		FSDataInputStream inputStream = fileSystem.open(path);

		builder = new StringBuilder();
		
		byte[] buffer = new byte[4096];
		int length;
		while ((length = inputStream.read(buffer)) != -1) {
			String string = new String(buffer, 0, length);
			builder.append(string);
		}
		
		inputStream.close();
		
		return builder;
	}

	public static BloomFilter loadBloomFilter(String prefix, int vectorSize,
			int nbHash, int hashType) throws IOException {
		String bloomName = getBloomName(prefix, vectorSize, nbHash, hashType);

		if (paramemetersToBloomFilter.containsKey(bloomName)) {
			// Bloom already initialized and in map, return it
			return paramemetersToBloomFilter.get(bloomName);
		} else {
			Path bloomPath = new Path(prefix + bloomName);
			
			FileSystem fileSystem = getFilesystem();
			
			if (fileSystem.exists(bloomPath)) {
				FSDataInputStream inputStream = fileSystem.open(bloomPath);

				BloomFilter bloomFilter = new BloomFilter(vectorSize, nbHash,
						hashType);
				bloomFilter.readFields(inputStream);
				
				inputStream.close();

				return bloomFilter;
			} else {
				return null;
			}
		}
	}
	
	private static FileSystem getFilesystem() throws IOException {
		Configuration config = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(prefix), config);
		return fileSystem;
	}

	public static void saveBloomFilter(BloomFilter bloomFilter, String prefix,
			int vectorSize, int nbHash, int hashType) throws IOException {
		String bloomName = getBloomName(prefix, vectorSize, nbHash, hashType);
		Path bloomFile = new Path(prefix + bloomName);

		FileSystem fileSystem = getFilesystem();
		
		if (!fileSystem.exists(bloomFile)) {
			// Bloom file doesn't exist yet, write it out
			FSDataOutputStream outputStream = fileSystem.create(bloomFile, true);
			bloomFilter.write(outputStream);
			outputStream.close();
		}
	}

	public static String getBloomName(String prefix, int vectorSize,
			int nbHash, int hashType) {
		return prefix + "_" + vectorSize + "_" + nbHash + "_" + hashType + "_" + QUOTE_SIZE
				+ ".bloom";
	}

	public static String[] breakUp(String input, int amount) {
		int size = input.length() - amount;

		String[] brokenUp = new String[size];

		for (int i = 0; i < size; i++) {
			brokenUp[i] = input.substring(i, i + amount);
		}

		return brokenUp;
	}

	public static void breakUp(BloomFilter filter, StringBuilder input,
			int amount) {
		int size = input.length() - amount;

		for (int i = 0; i < size; i++) {
			String value = input.substring(i, i + amount);
			Key key = new Key(value.getBytes());
			filter.add(key);

			if (i % 1000 == 0) {
				logger.info("Breaking up input " + i + "/" + size);
			}
		}
	}

	public static void getRandomBytes(byte[] randomBytes) {
		random.nextBytes(randomBytes);

		// Make bytes only a-z
		for (int i = 0; i < randomBytes.length; i++) {
			randomBytes[i] = (byte) ((Math.abs(randomBytes[i]) % LETTERS) + ASCII_OFFSET);
		}
	}
	
	public static String getStringForBytes(byte[] randomBytes) {
		return new String(randomBytes);
	}
}
