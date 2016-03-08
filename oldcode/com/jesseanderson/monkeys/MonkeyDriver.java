package com.jesseanderson.monkeys;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
 * The driver for the Hadoop project.  Creates the jobs, reads and writes out the BitSets, story text, and image files.
 */
public class MonkeyDriver extends Configured implements Tool {
	/** Stories loaded in memory */
	private Story[] stories = null;
	
	@Override
	public int run(String[] args) throws Exception {
		MonkeyUtils.logger.info("Starting");
		/*
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		*/
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");

		long iterations = 0;
		
		// Run forever, the stop file allows us to exit this loop
		while (true) {
			long iterationGroup = iterations * MonkeyMapper.MAP_ITERATION_SIZE;
			
			// Create new monkeys job
			JobConf conf = new JobConf(getConf(), getClass());
			conf.set( "mapred.child.java.opts", "-Xmx256m");
			conf.setJobName("Monkey Shakespeare");
			FileInputFormat.addInputPath(conf, new Path(MonkeyUtils.prefix + "input.txt"));
			
			MonkeyUtils.logger.info("Input created");
			
			String outputName = MonkeyUtils.prefix + "output/" + String.format("%s%sITER%20d", "output", dateFormat.format(new Date()), iterationGroup);
			Path outputPath = new Path(outputName);
			FileOutputFormat.setOutputPath(conf, outputPath);
			
			MonkeyUtils.logger.info("Output created");
			
			conf.setOutputKeyClass(LongWritable.class);
			conf.setOutputValueClass(StringArrayWritable.class);
			conf.setMapperClass(MonkeyMapper.class);
			conf.setCombinerClass(MonkeyReducer.class);
			conf.setReducerClass(MonkeyReducer.class);
		
			MonkeyUtils.logger.info("Running job");
			
			JobClient.runJob(conf);
			
			FileSystem fileSystem = FileSystem.get(URI.create(MonkeyUtils.prefix), conf);
			
			String status = "So far checked " + iterationGroup + "\n";
			
			FSDataOutputStream outputStream = fileSystem.create(new Path(MonkeyUtils.prefix + "total.txt"), true);
			outputStream.writeBytes(status);
			outputStream.close();
			
			System.out.print(status);
			
			loadStoriesAndBitSet(fileSystem);

			// Check to see if any job has any results
			FileStatus[] files = fileSystem.listStatus(outputPath);
			
			for (FileStatus fileStatus : files) {
				if (fileStatus.getPath().toString().contains("part-")) {
					if (fileStatus.getLen() != 0) {
						processPartFile(fileSystem, fileStatus.getPath());
						System.out.println("Found part file with non-zero.  File was:" + fileStatus.getPath().toString());
						
						break;
					}
				}
			}
			
			// Check for stop file, this is how I gracefully exit the infinite loop
			if (fileSystem.exists(new Path(MonkeyUtils.prefix + "stop.txt"))) {
				System.out.println("Stop file found ... stopping");
				break;
			}
			
			iterations++;
		}
		
		return 0;
	}

	/**
	 * Loads stories from text and loads their corresponding BitSet objects
	 * @fileSystem The fileSystem object to use for loading the files
	 */
	private void loadStoriesAndBitSet(FileSystem fileSystem) throws IOException {
		if (stories == null) {
			// Load stories into memory, to check for found strings
			Path storiesBase = new Path(MonkeyUtils.prefix + Story.STORIES_DIRECTORY);
			FileStatus[] storiesList = fileSystem.listStatus(storiesBase, new PathFilter() {
				@Override
				public boolean accept(Path arg0) {
					return arg0.getName().endsWith(Story.STORY_EXTENSION) && !arg0.getName().contains("total");
				}
			});
			
			stories = new Story[storiesList.length];
			
			for (int i = 0; i < storiesList.length; i++) {
				stories[i] = new Story();
				stories[i].name = storiesList[i].getPath().getName();
				stories[i].name = stories[i].name.substring(0, stories[i].name.indexOf("."));

				StringBuilder builder = MonkeyUtils.readEntireFile(storiesList[i].getPath());
				stories[i].story = builder.toString();
			}
			
			// Load story bit sets into memory
			FileStatus[] foundInStoriesList = fileSystem.listStatus(storiesBase, new PathFilter() {
				@Override
				public boolean accept(Path arg0) {
					return arg0.getName().endsWith(Story.BIT_SET_EXTENSION);
				}
			});
			
			MonkeyUtils.logger.info("Found " + storiesList.length + " stories and " + foundInStoriesList.length + " bit sets. In path \"" + 
					storiesBase.toUri() + "\"");
			
			if (foundInStoriesList.length == 0) {
				// Create new bit set files
				for (int i = 0; i < storiesList.length; i++) {
					FSDataOutputStream outputStream = fileSystem.create(new Path(stories[i].getBitSetFile()), true);
					outputStream.close();
					
					stories[i].bitSet = new BitSet();
				}
			} else if (foundInStoriesList.length == storiesList.length) {
				// Load in existing bit set files
				for (int i = 0; i < storiesList.length; i++) {
					try {
						if (fileSystem.getFileStatus(new Path(stories[i].getBitSetFile())).getLen() != 0) {
							FSDataInputStream inputStream = fileSystem.open(new Path(stories[i].getBitSetFile()));
							ObjectInputStream s = new ObjectInputStream(inputStream); 
					        BitSet bitSet = (BitSet) s.readObject(); 
							inputStream.close();
							
							stories[i].bitSet = bitSet;
						} else {
							FSDataOutputStream outputStream = fileSystem.create(new Path(stories[i].getBitSetFile()), true);
							outputStream.close();
							
							stories[i].bitSet = new BitSet();
						}
					} catch (Exception e) {
						MonkeyUtils.logger.error("Error while reading in BitSet", e);
					}
				}
			} else {
				MonkeyUtils.logger.fatal("Story and bit set sizes do not match up!");
				return;
			}
		}
	}
	
	/**
	 * Example Output:
	 * 2000000	tonybyse,
	 */
	private static Pattern foundStringsPattern = Pattern.compile("\\d*\\s*(\\w*),\\s*");
	
	/*
	 * Processes the part file to see which strings were found.  Updates the BitSets.
	 * Creates the images and html table file
	 * @param fileSystem The file system object to load the files from
	 * @param foundStringsPath The path to the part file
	 */
	private void processPartFile(FileSystem fileSystem, Path foundStringsPath) {
		StringBuilder builder;
		
		try {
			builder = MonkeyUtils.readEntireFile(foundStringsPath);
		} catch (Exception e) {
			MonkeyUtils.logger.error("Unable to read found strings file", e);
			return;
		}
		
		String[] lines = builder.toString().split("\n");
		
		// Go through all found strings
		for (String line : lines) {
			Matcher m = foundStringsPattern.matcher(line);
			
			if (m.matches()) {
				String foundString = m.group(1);
				
				// See which story the string is found in
				for (int i = 0; i < stories.length; i++) {
					// TODO: This part is inefficient and should use Lucene or something faster than indexOf
					int startIndex = -1;
					
					// Update that story's BitSet
					while ((startIndex = stories[i].story.indexOf(foundString, startIndex + 1)) != -1) {
						for (int j = 0; j < foundString.length(); j++) {
							stories[i].bitSet.set(startIndex + j);
						}
					}
				}
			} else {
				MonkeyUtils.logger.info("Did not match regex.  Line was \"" + line + "\"");
			}
		}
		
		ImageGenerator generator = new ImageGenerator();
		
		try {
			Path totalsPath = new Path(MonkeyUtils.prefix + Story.STORIES_DIRECTORY + "totals.xml");
			FSDataOutputStream totalsOutputStream = fileSystem.create(totalsPath, true);
			
			totalsOutputStream.writeBytes("<table><tr><td>Title</td><td>Percent Found</td><td>Total Chars. Found</td><td>Total Chars.</td><td>Chars. Left</td></tr>");
		
			// Output new BitSet objects and to foundStringsPath to keep a record of the BitSet at that point
			for (int i = 0; i < stories.length; i++) {
				// Output new BitSet objects to save overall state
				FSDataOutputStream outputStream = fileSystem.create(new Path(stories[i].getBitSetFile()), true);
				ObjectOutputStream s = new ObjectOutputStream(outputStream); 
		        s.writeObject(stories[i].bitSet);
				outputStream.close();
				
				// Output image of BitSet
				outputStream = fileSystem.create(new Path(stories[i].getImageFile()), true);
				int found = generator.createImage(stories[i].bitSet, stories[i].story.length(), outputStream);
				outputStream.close();
				
				// Output to foundStringsPath to keep a record of the BitSet and image at that point
				String baseToPath = foundStringsPath.getParent().toString() + "/" + stories[i].name;
				copyFile(new Path(stories[i].getBitSetFile()), new Path(baseToPath + Story.BIT_SET_EXTENSION), fileSystem);
				
				Path imagePath = new Path(baseToPath + Story.IMAGE_EXTENSION);
				copyFile(new Path(stories[i].getImageFile()), imagePath, fileSystem);
				
				// Copy current to local filesystem
				fileSystem.copyToLocalFile(imagePath, new Path("currentstories/" + imagePath.getName()));
				
				int storySize = stories[i].story.length();
				float percentDone = (((float)found / (float) storySize) * 100f);
				int charactersLeft = storySize - found; 
				
				String htmlTotalOuput = String.format( "<tr><td>%s</td><td>%.3f%%</td><td>%,d</td><td>%,d</td><td>%d</td></tr>", stories[i].name, 
						percentDone, found, storySize, charactersLeft);
				totalsOutputStream.writeBytes(htmlTotalOuput + "\n");
				
				String loggerTotalOuput = String.format( "For %s found so far %.3f%% Found: %,d Total: %,d Left: %,d", stories[i].name, 
						percentDone, found, storySize, charactersLeft);
				MonkeyUtils.logger.info(loggerTotalOuput);
			}
			
			totalsOutputStream.writeBytes("</table>");
			
			totalsOutputStream.close();
			
			// Copy totals file
			copyFile(totalsPath, new Path(foundStringsPath.getParent().toString() + totalsPath.getName()), fileSystem);
			fileSystem.copyToLocalFile(totalsPath, new Path("currentstories/" + totalsPath.getName()));
		} catch (Exception e) {
			MonkeyUtils.logger.error("Unable to write new BitSet file", e);
		}
	}
	
	/**
	 * Copies a file from one path to another
	 * @param from The source file
	 * @param to The destination file
	 * @param fileSystem The file system object to load the files from
	 */
	private void copyFile(Path from, Path to, FileSystem fileSystem) throws IOException {
		FSDataInputStream inputStream = fileSystem.open(from);		
		FSDataOutputStream outputStream = fileSystem.create(to, true);
		
		int read;
		byte[] buffer = new byte[4096];
		
		while ((read = inputStream.read(buffer)) != -1) {
			outputStream.write(buffer, 0, read);
		}
		
		inputStream.close();
		outputStream.close();
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MonkeyDriver(), args);
		System.exit(exitCode);
	}
}
