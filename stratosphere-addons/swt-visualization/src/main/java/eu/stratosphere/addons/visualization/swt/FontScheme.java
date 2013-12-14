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

package eu.stratosphere.addons.visualization.swt;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.Font;

public class FontScheme {

	private final Font TOOLTIPTITLEFONT;

	private final Font GROUPVERTEXFONT;

	private static FontScheme fontSchemeInstance = null;

	private FontScheme(Device device) {

		this.TOOLTIPTITLEFONT = new Font(device, "Arial", 10, SWT.BOLD);
		this.GROUPVERTEXFONT = new Font(device, "Arial", 12, SWT.BOLD);
	}

	private static FontScheme getInstance(Device device) {

		if (fontSchemeInstance == null) {
			fontSchemeInstance = new FontScheme(device);
		}

		return fontSchemeInstance;
	}

	public static Font getToolTipTitleFont(Device device) {

		return getInstance(device).TOOLTIPTITLEFONT;
	}

	public static Font getGroupVertexFont(Device device) {

		return getInstance(device).GROUPVERTEXFONT;
	}
}
