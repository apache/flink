/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.addons.visualization.swt;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.Point;

import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;

public class SWTGroupVertex extends AbstractSWTVertex {

	private final static int SPACING = 5;

	private final static int NAMERECTWIDTH = 50;

	private final static int ARC_DIM = 20;

	private final ManagementGroupVertex managementGroupVertex;

	public SWTGroupVertex(AbstractSWTComponent parent, ManagementGroupVertex managementGroupVertex) {
		super(parent);

		this.managementGroupVertex = managementGroupVertex;
	}

	@Override
	public void layout() {

		final int numberOfChildren = getNumberOfChildren();
		if (numberOfChildren == 0) {
			return;
		}
		final int width = (getWidth() - NAMERECTWIDTH) / numberOfChildren - (2 * SPACING);
		final int height = getHeight() - (2 * SPACING);
		final int y = getY() + SPACING;

		for (int i = 0; i < numberOfChildren; i++) {
			AbstractSWTComponent child = getChildAt(i);
			final int x = getX() + NAMERECTWIDTH + ((i + 1) * SPACING) + (i * (width + SPACING));
			child.setX(x);
			child.setY(y);
			child.setWidth(width);
			child.setHeight(height);
		}

		layoutChildren();
	}

	/*
	 * private void paintEdge(GC gc, Device device, int sourceX, int sourceY, int targetX, int targetY) {
	 * gc.drawLine(sourceX, sourceY, targetX, targetY);
	 * }
	 */

	private String cropStringToSize(GC gc, String originalString, int maximumWidth) {

		String croppedString = originalString;
		boolean cropped = false;

		Point pt = gc.textExtent(originalString);
		while (pt.x >= maximumWidth) {
			cropped = true;

			croppedString = croppedString.substring(0, croppedString.length() - 1);
			if (croppedString.length() == 0) {
				break;
			}

			pt = gc.textExtent(croppedString + "...");
		}

		if (cropped) {
			croppedString += "...";
		}

		return croppedString;
	}

	@Override
	public boolean isSelectable() {
		return true;
	}

	@Override
	protected void paintInternal(GC gc, Device device) {

		gc.setBackground(ColorScheme.getGroupVertexBackgroundColor(device));
		gc.fillRoundRectangle(this.rect.x, this.rect.y, this.rect.width, this.rect.height, ARC_DIM, ARC_DIM);

		final int oldWidth = gc.getLineWidth();
		gc.setLineWidth(6);

		gc.setForeground(ColorScheme.getGroupVertexBorderColor(device));

		// Draw name of the task vertically
		drawVerticalText(gc, this.managementGroupVertex.getName());

		/*
		 * Iterator<AbstractSWTVertex> it = getEdges();
		 * int sourceX = getEdgeSourceX();
		 * int sourceY = getEdgeSourceY();
		 * while(it.hasNext()) {
		 * AbstractSWTVertex target = it.next();
		 * paintEdge(gc, device, sourceX, sourceY, target.getEdgeTargetX(), target.getEdgeTargetY());
		 * }
		 */

		// Reset line width
		gc.setLineWidth(oldWidth);

	}

	private void drawVerticalText(GC gc, String text) {

		// Determine dimension of image
		final Font oldFont = gc.getFont();
		gc.setFont(FontScheme.getGroupVertexFont(gc.getDevice()));
		final int maximumWidth = this.getHeight() - (2 * SPACING);
		final String croppedText = cropStringToSize(gc, text, maximumWidth);
		final Point pt = gc.stringExtent(croppedText);
		gc.setFont(oldFont);

		final Image textImage = new Image(gc.getDevice(), pt.x, pt.y);
		final GC textGC = new GC(textImage);
		textGC.setBackground(gc.getBackground());
		textGC.fillRectangle(textImage.getBounds());
		textGC.setFont(FontScheme.getGroupVertexFont(textGC.getDevice()));
		textGC.drawText(croppedText, 0, 0);

		final Image rotatedImage = rotateImage(gc.getDevice(), textImage);

		gc.drawImage(rotatedImage, this.rect.x + SPACING + ((NAMERECTWIDTH - pt.y) / 2), this.rect.y + SPACING
			+ ((maximumWidth - pt.x) / 2));

		// Clean up
		rotatedImage.dispose();
		textGC.dispose();
		textImage.dispose();
	}

	private static Image rotateImage(Device device, Image originalImage) {

		final ImageData sd = originalImage.getImageData();
		final ImageData dd = new ImageData(sd.height, sd.width, sd.depth, sd.palette);

		for (int sx = 0; sx < sd.width; sx++) {
			// Run through the vertical pixels
			for (int sy = 0; sy < sd.height; sy++) {
				dd.setPixel(sy, sd.width - sx - 1, sd.getPixel(sx, sy));
			}
		}

		return new Image(device, dd);
	}
}
