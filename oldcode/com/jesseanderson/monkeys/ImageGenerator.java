package com.jesseanderson.monkeys;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.BitSet;

import javax.imageio.ImageIO;

import org.apache.hadoop.fs.FSDataOutputStream;

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

public class ImageGenerator {
	/** The maximum width of the image output */
	private static final int MAX_WIDTH = 720;
	/** The number of trues found in the BitSet */
	private int found;

	/**
	 * Creates an image in the output stream using the BitSet
	 * @bitSet The BitSet containing the data to output
	 * @size The size of the work being output
	 * @outputStream The OutputStream for writing the image to
	 * @return The number of trues in the BitSet
	 */
	public int createImage(BitSet bitSet, int size, FSDataOutputStream outputStream) {
		found = 0;
		
		final BufferedImage img = map(bitSet, size);
		savePNG(img, outputStream);
		
		return found;
	}

	/**
	 * Creates the image output using the BitSet
	 * @bitSet The BitSet containing the data to output
	 * @size The size of the work being output
	 * @return A PNG image written to a BufferedImage
	 */
	private BufferedImage map(BitSet bitSet, int size) {
		int height = size / MAX_WIDTH;
		
		if (size % MAX_WIDTH != 0) {
			// Add an extra line to accommodate the last row of pixels
			height++;
		}

		final BufferedImage res = new BufferedImage(MAX_WIDTH, height, BufferedImage.TYPE_BYTE_INDEXED);
		
		// Go through every bit in the BitSet to see if the work was found
		for (int y = 0; y < height; y++) {
			for (int x = 0; x < MAX_WIDTH; x++) {
				int index = (y * MAX_WIDTH) + x;
				Color color;
				
				if (bitSet.get(index)) {
					// Found a hit
					color = Color.GREEN;
					found++;
				} else {
					// Hasn't found this one yet
					color = Color.WHITE;
				}
				
				res.setRGB(x, y, color.getRGB());
			}
		}
		return res;
	}

	/**
	 * Saves the BufferedImage to the OutputStream
	 * @param bi The BufferedImage containing the PNG
	 * @param outputStream The object to write the BufferedImage to
	 */
	private void savePNG(final BufferedImage bi, FSDataOutputStream outputStream) {
		try {
			ImageIO.write(bi, "PNG", outputStream);
			outputStream.close();
		} catch (IOException e) {
			MonkeyUtils.logger.error("Error while writing png", e);
		}
	}
}
