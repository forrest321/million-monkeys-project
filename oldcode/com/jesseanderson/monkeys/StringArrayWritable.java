package com.jesseanderson.monkeys;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

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
 * Allows an iteration's strings to be kept in an array
 * @author jesse
 */
public class StringArrayWritable extends ArrayWritable {
	public StringArrayWritable() {
		super(Text.class);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StringArrayWritable) {
			StringArrayWritable comparison = (StringArrayWritable) obj;
			
			if (comparison.get().length == get().length) {
				Writable[] thisWritables = get();
				Writable[] comparisonWritables = get();
				
				for (int i = 0; i < comparisonWritables.length; i++) {
					if (!comparisonWritables[i].equals(thisWritables[i])) {
						return false;
					}
				}
				
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		if (get().length != 0) {
			StringBuilder builder = new StringBuilder();
			
			for (int i = 0; i < get().length; i++) {
				builder.append(get()[i]).append(", ");
			}
			
			return builder.toString();
		} else {
			return "";
		}
	}
}
