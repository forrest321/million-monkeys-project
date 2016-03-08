package com.jesseanderson.monkeys;

import java.util.BitSet;

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
 * Model object to aid in keeping the stories and their data together
 */
public class Story {
	/** The sub-directory path with the stories */
	public static final String STORIES_DIRECTORY = "stories/";
	/** The file extension for the story text */
	public static final String STORY_EXTENSION = ".txt";
	/** The file extension for the BitSet object */
	public static final String BIT_SET_EXTENSION = ".bit";
	/** The file extension for the image output */
	public static final String IMAGE_EXTENSION = ".png";
	
	/** The text of the story */
	public String story;
	/** The BitSet for the story */
	public BitSet bitSet;
	/** The name of the story */
	public String name;
	
	/**
	 * Gets the file path for the story file
	 * @return The file path for the story file
	 */
	public String getStoryFile() {
		return MonkeyUtils.prefix + Story.STORIES_DIRECTORY + name + STORY_EXTENSION;
	}
	
	/**
	 * Gets the file path for the BitSet file
	 * @return The file path for the BitSet file
	 */
	public String getBitSetFile() {
		return MonkeyUtils.prefix + Story.STORIES_DIRECTORY + name + BIT_SET_EXTENSION;
	}

	/**
	 * Gets the file path for the image file
	 * @return The file path for the image file
	 */		
	public String getImageFile() {
		return MonkeyUtils.prefix + Story.STORIES_DIRECTORY + name + IMAGE_EXTENSION;
	}
	
	@Override
	public String toString() {
		return name;
	}
}
