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

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.swt.SWTException;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.widgets.Canvas;
import org.eclipse.swt.widgets.Composite;

import org.eclipse.swt.graphics.Rectangle;

public class LogoCanvas extends Canvas implements PaintListener {

	private final Image stratosphereLogo;

	private final Image stratospherePattern;

	private final Rectangle stratosphereLogoRect;

	private final Rectangle stratospherePatternRect;

	private Point preferredSize = null;

	public LogoCanvas(Composite parent, int style) {
		super(parent, style);

		// Try to locate and load image for the logo
		Image stratosphereLogoTmp = null;
		Rectangle stratosphereLogoRectTmp = null;
		InputStream in = getClass().getResourceAsStream("/eu/stratosphere/nephele/visualization/swt/logo.png");
		if (in != null) {

			try {
				stratosphereLogoTmp = loadImage(in);
				stratosphereLogoRectTmp = stratosphereLogoTmp.getBounds();
			} catch (SWTException e) {
				e.printStackTrace();
				stratosphereLogoRectTmp = new Rectangle(0, 0, 0, 0);
			} finally {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		} else {
			stratosphereLogoRectTmp = new Rectangle(0, 0, 0, 0);
		}
		this.stratosphereLogo = stratosphereLogoTmp;
		this.stratosphereLogoRect = stratosphereLogoRectTmp;

		// Try to locate and load image for the pattern
		Image stratospherePatternTmp = null;
		Rectangle stratospherePatternRectTmp = null;
		in = getClass().getResourceAsStream("/eu/stratosphere/nephele/visualization/swt/pattern.png");
		if (in != null) {

			try {
				stratospherePatternTmp = loadImage(in);
				stratospherePatternRectTmp = stratospherePatternTmp.getBounds();
			} catch (SWTException e) {
				e.printStackTrace();
				stratospherePatternRectTmp = new Rectangle(0, 0, 0, 0);
			} finally {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		} else {
			stratospherePatternRectTmp = new Rectangle(0, 0, 0, 0);
		}
		this.stratospherePattern = stratospherePatternTmp;
		this.stratospherePatternRect = stratospherePatternRectTmp;

		this.addPaintListener(this);
	}

	private Image loadImage(InputStream inputStream) {

		try {
			return new Image(getDisplay(), inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public void paintControl(PaintEvent arg0) {

		final GC gc = arg0.gc;

		final Rectangle rect = getBounds();

		gc.setBackground(ColorScheme.getLogoCanvasBackgroundColor(this.getDisplay()));
		gc.fillRectangle(rect);

		if (this.stratosphereLogo != null) {
			if (this.stratosphereLogoRect.height <= rect.height && this.stratosphereLogoRect.width <= rect.width) {
				gc.drawImage(this.stratosphereLogo, rect.x, rect.y);
			}
		}

		if (this.stratospherePattern != null) {

			if (this.stratospherePatternRect.height <= rect.height) {
				if (this.stratosphereLogo == null) {
					if (this.stratospherePatternRect.width <= rect.width) {
						gc.drawImage(this.stratospherePattern,
							rect.x + rect.width - this.stratospherePatternRect.width, rect.y);
					}
				} else {
					if ((this.stratosphereLogoRect.width + this.stratospherePatternRect.width) <= rect.width) {
						gc.drawImage(this.stratospherePattern,
							rect.x + rect.width - this.stratospherePatternRect.width, rect.y);
					}
				}
			}
		}
	}

	@Override
	public Point computeSize(int wHint, int hHint, boolean changed) {

		if (this.preferredSize == null || changed) {
			if (this.stratospherePattern != null) {
				this.preferredSize = new Point(wHint, this.stratospherePattern.getBounds().height);
			} else {
				this.preferredSize = new Point(wHint, hHint);
			}
		}

		return this.preferredSize;
	}
}
