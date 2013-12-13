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

import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

public class SWTImageCanvas extends Canvas implements PaintListener {

	private final Image image;

	private final Rectangle imageRect;

	public SWTImageCanvas(Composite parent, int style, Image image) {
		super(parent, style);

		this.image = image;
		this.imageRect = image.getBounds();

		this.addPaintListener(this);
	}

	@Override
	public void paintControl(PaintEvent arg0) {

		Rectangle rect = getClientArea();

		arg0.gc.drawImage(this.image, this.imageRect.x, this.imageRect.y, this.imageRect.width, this.imageRect.height,
			rect.x, rect.y, rect.width, rect.height);

	}

}
