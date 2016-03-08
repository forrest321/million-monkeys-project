package com.jesseanderson.monkeys;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

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
 * Utility program to take the Project Gutenburg file and remove Gutenburg's license.  Shakespeare didn't write 
 * this and the monkeys don't need to either.  Save out the individual works in their own files.
 */
public class Formatter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader("pg100.txt"));

			StringBuilder output = new StringBuilder();
			String line = null;

			ArrayList<String> lines = new ArrayList<String>();

			HashMap<String, StringBuilder> storyToText = new HashMap<String, StringBuilder>();
			StringBuilder currentStory = new StringBuilder();
			String previousStoryName = "";

			while ((line = reader.readLine()) != null) {
				lines.add(line);
				line = line.trim().toLowerCase();

				char[] characters = line.toCharArray();

				if (line.equals("by william shakespeare")) {
					String storyName = lines.get(lines.size() - 3);

					// Search around for the story name
					if (storyName.length() == 0) {
						storyName = lines.get(lines.size() - 4);
					}

					if (storyName.length() == 0) {
						storyName = lines.get(lines.size() - 2);
					}

					char[] storyChars = storyName.toLowerCase().toCharArray();
					storyName = "";

					for (char character : storyChars) {
						if ((character >= 97 && character <= 122) || character == 32) {
							storyName += character;
						}
					}
					
					storyName = capitalizeString(storyName);
					
					// Remove last story name from currentStory
					currentStory.setLength(currentStory.length() - previousStoryName.length());
					
					currentStory = new StringBuilder();
					storyToText.put(storyName, currentStory);
					
					previousStoryName = storyName;
					
					continue;
				}

				if (line.equals("<<THIS ELECTRONIC VERSION OF THE COMPLETE WORKS OF WILLIAM")
						|| line.equals("SHAKESPEARE IS COPYRIGHT 1990-1993 BY WORLD LIBRARY, INC., AND IS")
						|| line.equals("PROVIDED BY PROJECT GUTENBERG ETEXT OF ILLINOIS BENEDICTINE COLLEGE")
						|| line.equals("WITH PERMISSION.  ELECTRONIC AND MACHINE READABLE COPIES MAY BE")
						|| line.equals("DISTRIBUTED SO LONG AS SUCH COPIES (1) ARE FOR YOUR OR OTHERS")
						|| line.equals("PERSONAL USE ONLY, AND (2) ARE NOT DISTRIBUTED OR USED")
						|| line.equals("COMMERCIALLY.  PROHIBITED COMMERCIAL DISTRIBUTION INCLUDES BY ANY")
						|| line.equals("SERVICE THAT CHARGES FOR DOWNLOAD TIME OR FOR MEMBERSHIP.>>")) {
					continue;
				}

				for (char character : characters) {
					if (character >= 97 && character <= 122) {
						output.append(character);
						currentStory.append(character);
					}
				}

				// System.out.println(line);
			}

			BufferedWriter writer = new BufferedWriter(new FileWriter("stories/pg100_out.txt"));
			writer.append(output);
			writer.close();

			for (String storyName : storyToText.keySet()) {
				BufferedWriter storyWriter = new BufferedWriter(new FileWriter("stories/" + storyName + ".txt"));
				storyWriter.append(storyToText.get(storyName));
				storyWriter.close();
			}

			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String capitalizeString(String string) {
		char[] chars = string.toLowerCase().toCharArray();
		boolean found = false;
		for (int i = 0; i < chars.length; i++) {
			if (!found && Character.isLetter(chars[i])) {
				chars[i] = Character.toUpperCase(chars[i]);
				found = true;
			} else if (Character.isWhitespace(chars[i]) || chars[i] == '.' || chars[i] == '\'') { // You
																									// can
																									// add
																									// other
																									// chars
																									// here
				found = false;
			}
		}
		return String.valueOf(chars);
	}

}
