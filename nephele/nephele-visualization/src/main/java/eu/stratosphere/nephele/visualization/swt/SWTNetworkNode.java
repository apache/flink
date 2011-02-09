/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.visualization.swt;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.topology.NetworkNode;

public class SWTNetworkNode extends AbstractSWTComponent {

	public final static int MAXWIDTH = 200;

	public final static int MAXHEIGHT = 120;

	private final static int TEXTBOXHEIGHT = 24;

	private final static int TEXTBOXSPACING = 10;

	private static Image LEAFNODEIMAGE;

	private final Image leafNodeImage;

	private final Rectangle leafNodeImageRect;

	private static Rectangle LEAFNODEIMAGERECT;

	private static Image NODEIMAGE;

	private static Rectangle NODEIMAGERECT;

	private final Image nodeImage;

	private final Rectangle nodeImageRect;

	private final NetworkNode networkNode;

	private SWTNetworkNode parentNode = null;

	private List<SWTNetworkNode> childNodes = new ArrayList<SWTNetworkNode>();

	private int depth = -1;

	private float imageScaleFactor = 1.0f;

	public SWTNetworkNode(Display display, AbstractSWTComponent parentComponent, NetworkNode networkNode) {
		super(parentComponent);

		this.networkNode = networkNode;

		synchronized (SWTNetworkNode.class) {

			if (LEAFNODEIMAGE == null) {

				// Try to load the images
				InputStream in = getClass().getResourceAsStream(
					"/eu/stratosphere/nephele/visualization/swt/leafnode.png");
				try {
					LEAFNODEIMAGE = new Image(display, in);
					LEAFNODEIMAGERECT = LEAFNODEIMAGE.getBounds();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			this.leafNodeImage = LEAFNODEIMAGE;
			this.leafNodeImageRect = LEAFNODEIMAGERECT;

			if (NODEIMAGE == null) {
				// Try to load the images
				InputStream in = getClass().getResourceAsStream("/eu/stratosphere/nephele/visualization/swt/node.png");
				try {
					NODEIMAGE = new Image(display, in);
					NODEIMAGERECT = NODEIMAGE.getBounds();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			this.nodeImage = NODEIMAGE;
			this.nodeImageRect = NODEIMAGERECT;
		}
	}

	void setParentNode(SWTNetworkNode parentNode) {

		this.parentNode = parentNode;
		this.parentNode.addChildNode(this);
	}

	protected void addChildNode(SWTNetworkNode childNode) {
		this.childNodes.add(childNode);
	}

	public NetworkNode getNetworkNode() {
		return this.networkNode;
	}

	@Override
	public boolean isSelectable() {

		return true;
	}

	public int getDepth() {

		// Cache value
		if (this.depth < 0) {
			this.depth = this.networkNode.getDepth();
		}

		return this.depth;
	}

	int getNumberOfNodesOnLevel(int level) {

		int numberOfNodesOnLevel = 0;
		if (level == getDepth()) {
			++numberOfNodesOnLevel;
		}

		if (level > getDepth()) {
			// Consider children;
			final Iterator<SWTNetworkNode> it = this.childNodes.iterator();
			while (it.hasNext()) {
				numberOfNodesOnLevel += it.next().getNumberOfNodesOnLevel(level);
			}
		}

		return numberOfNodesOnLevel;
	}

	@Override
	public void layout() {

		// Adapt scale factor
		if (this.networkNode.isLeafNode()) {
			if (this.leafNodeImage != null) {
				this.imageScaleFactor = (float) (this.rect.height - TEXTBOXHEIGHT)
					/ (float) this.leafNodeImageRect.height;
			}
		} else {
			if (this.nodeImage != null) {
				this.imageScaleFactor = (float) (this.rect.height - TEXTBOXHEIGHT) / (float) this.nodeImageRect.height;
			}
		}
	}

	@Override
	protected void paintInternal(GC gc, Device device) {

		// Draw edge to parent
		if (this.parentNode != null) {
			gc.drawLine(this.rect.x + (rect.width / 2), this.rect.y + (this.rect.height - TEXTBOXHEIGHT) / 2,
				this.parentNode.getX() + (this.parentNode.getWidth() / 2), this.parentNode.getY()
					+ this.parentNode.getHeight());
		}

		if (this.networkNode.isLeafNode()) {
			if (this.leafNodeImage != null) {

				final int destHeight = (int) ((float) this.leafNodeImageRect.height * imageScaleFactor);
				final int destWidth = (int) ((float) this.leafNodeImageRect.width * imageScaleFactor);
				final int destX = this.rect.x + (this.rect.width - destWidth) / 2;
				final int destY = this.rect.y + ((this.rect.height - TEXTBOXHEIGHT) - destHeight) / 2;

				gc.drawImage(this.leafNodeImage, this.leafNodeImageRect.x, this.leafNodeImageRect.y,
					this.leafNodeImageRect.width, this.leafNodeImageRect.height, destX, destY, destWidth, destHeight);

			}
		} else {
			if (this.nodeImage != null) {
				final int destHeight = (int) ((float) this.nodeImageRect.height * imageScaleFactor);
				final int destWidth = (int) ((float) this.nodeImageRect.width * imageScaleFactor);
				final int destX = this.rect.x + (this.rect.width - destWidth) / 2;
				final int destY = this.rect.y + ((this.rect.height - TEXTBOXHEIGHT) - destHeight) / 2;

				gc.drawImage(this.nodeImage, this.nodeImageRect.x, this.nodeImageRect.y, this.nodeImageRect.width,
					this.nodeImageRect.height, destX, destY, destWidth, destHeight);
			}
		}

		// Determine width of text box
		String name = this.networkNode.getName();
		Point pt = gc.textExtent(name);
		boolean cropped = false;
		while (pt.x > this.rect.width) {
			cropped = true;

			name = name.substring(0, name.length() - 1);

			pt = gc.textExtent(name + "...");
		}

		if (cropped) {
			name += "...";
		}

		// Draw text box
		gc.setBackground(ColorScheme.getNetworkNodeBackgroundColor(device));
		gc.fillRectangle(this.rect.x + (this.rect.width - pt.x) / 2 - TEXTBOXSPACING, this.rect.y + this.rect.height
			- TEXTBOXHEIGHT, pt.x + (2 * TEXTBOXSPACING), TEXTBOXHEIGHT);
		gc.setForeground(ColorScheme.getNetworkNodeBorderColor(device));
		gc.drawRectangle(this.rect.x + (this.rect.width - pt.x) / 2 - TEXTBOXSPACING, this.rect.y + this.rect.height
			- TEXTBOXHEIGHT, pt.x + (2 * TEXTBOXSPACING), TEXTBOXHEIGHT);

		// Draw name
		gc.setForeground(ColorScheme.getNetworkNodeBorderColor(device));
		final int textX = this.rect.x + ((this.rect.width - pt.x) / 2);
		final int textY = this.rect.y + this.rect.height - TEXTBOXHEIGHT + ((TEXTBOXHEIGHT - pt.y) / 2);
		gc.drawText(name, textX, textY);
	}

	@Override
	public SWTToolTip constructToolTip(Shell parentShell, SWTToolTipCommandReceiver commandReceiver, int x, int y) {

		return new SWTInstanceToolTip(parentShell, commandReceiver, this.networkNode, x, y);
	}
}
