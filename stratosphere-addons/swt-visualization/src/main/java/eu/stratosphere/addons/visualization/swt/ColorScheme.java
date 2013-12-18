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

import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;

public class ColorScheme {

	private final Color LOGOCANVASBACKGROUNDCOLOR;

	private final Color GRAPHBACKGROUNDCOLOR;

	// Vertex colors
	private final Color VERTEXDEFAULTBACKGROUNDCOLOR;

	private final Color VERTEXRUNNINGBACKGROUNDCOLOR;

	private final Color VERTEXREPLAYINGBACKGROUNDCOLOR;

	private final Color VERTEXFINISHINGBACKGROUNDCOLOR;

	private final Color VERTEXFINISHEDBACKGROUNDCOLOR;

	private final Color VERTEXCANCELBACKGROUNDCOLOR;

	private final Color VERTEXFAILEDBACKGROUNDCOLOR;

	// Gate colors
	private final Color GATEDEFAULTBORDERCOLOR;

	private final Color GATEDEFAULTBACKGROUNDCOLOR;

	private final Color GATERUNNINGBORDERCOLOR;

	private final Color GATERUNNINGBACKGROUNDCOLOR;

	private final Color GATEREPLAYINGBORDERCOLOR;

	private final Color GATEREPLAYINGBACKGROUNDCOLOR;

	private final Color GATEFINISHINGBORDERCOLOR;

	private final Color GATEFINISHINGBACKGROUNDCOLOR;

	private final Color GATEFINISHEDBORDERCOLOR;

	private final Color GATEFINISHEDBACKGROUNDCOLOR;

	private final Color GATECANCELBORDERCOLOR;

	private final Color GATECANCELBACKGROUNDCOLOR;

	private final Color GATEFAILEDBORDERCOLOR;

	private final Color GATEFAILEDBACKGROUNDCOLOR;

	private final Color GROUPVERTEXBACKGROUNDCOLOR;

	private final Color GROUPVERTEXBORDERCOLOR;

	private final Color STAGEBACKGROUNDCOLOR;

	private final Color STAGEBORDERCOLOR;

	private final Color NETWORKTOPOLOGYBACKGROUNDCOLOR;

	private final Color NETWORKNODEBORDERCOLOR;

	private final Color NETWORKNODEBACKGROUNDCOLOR;

	private static ColorScheme colorSchemeInstance = null;

	private ColorScheme(Device device) {

		this.LOGOCANVASBACKGROUNDCOLOR = new Color(device, 255, 255, 255);
		this.GRAPHBACKGROUNDCOLOR = new Color(device, 155, 193, 255);

		this.VERTEXDEFAULTBACKGROUNDCOLOR = new Color(device, 162, 162, 162);
		this.VERTEXRUNNINGBACKGROUNDCOLOR = new Color(device, 155, 187, 89);
		this.VERTEXREPLAYINGBACKGROUNDCOLOR = new Color(device, 155, 187, 89);
		this.VERTEXFINISHINGBACKGROUNDCOLOR = new Color(device, 130, 190, 255);
		this.VERTEXFINISHEDBACKGROUNDCOLOR = new Color(device, 93, 121, 246);
		this.VERTEXCANCELBACKGROUNDCOLOR = new Color(device, 247, 150, 70);
		this.VERTEXFAILEDBACKGROUNDCOLOR = new Color(device, 229, 36, 36);

		this.GATEDEFAULTBORDERCOLOR = new Color(device, 162, 162, 162);
		this.GATEDEFAULTBACKGROUNDCOLOR = new Color(device, 104, 104, 104);
		this.GATERUNNINGBORDERCOLOR = new Color(device, 155, 187, 89);
		this.GATERUNNINGBACKGROUNDCOLOR = new Color(device, 95, 114, 54);
		this.GATEREPLAYINGBORDERCOLOR = new Color(device, 155, 187, 89);
		this.GATEREPLAYINGBACKGROUNDCOLOR = new Color(device, 95, 114, 54);
		this.GATEFINISHINGBORDERCOLOR = new Color(device, 130, 190, 255);
		this.GATEFINISHINGBACKGROUNDCOLOR = new Color(device, 95, 135, 250);
		this.GATEFINISHEDBORDERCOLOR = new Color(device, 93, 121, 246);
		this.GATEFINISHEDBACKGROUNDCOLOR = new Color(device, 48, 63, 128);
		this.GATECANCELBORDERCOLOR = new Color(device, 247, 150, 70);
		this.GATECANCELBACKGROUNDCOLOR = new Color(device, 178, 108, 50);
		this.GATEFAILEDBORDERCOLOR = new Color(device, 229, 36, 36);
		this.GATEFAILEDBACKGROUNDCOLOR = new Color(device, 166, 27, 27);

		this.GROUPVERTEXBACKGROUNDCOLOR = new Color(device, 210, 210, 210);
		this.GROUPVERTEXBORDERCOLOR = new Color(device, 89, 89, 89);

		this.STAGEBACKGROUNDCOLOR = new Color(device, 255, 255, 255);
		this.STAGEBORDERCOLOR = new Color(device, 0, 0, 0);
		this.NETWORKTOPOLOGYBACKGROUNDCOLOR = new Color(device, 155, 193, 255);
		this.NETWORKNODEBACKGROUNDCOLOR = new Color(device, 255, 255, 255);
		this.NETWORKNODEBORDERCOLOR = new Color(device, 0, 0, 0);
	}

	private static ColorScheme getInstance(Device device) {

		if (colorSchemeInstance == null) {
			colorSchemeInstance = new ColorScheme(device);
		}

		return colorSchemeInstance;
	}

	public static Color getLogoCanvasBackgroundColor(Device device) {
		return getInstance(device).LOGOCANVASBACKGROUNDCOLOR;
	}

	public static Color getVertexFinishingBackgroundColor(Device device) {
		return getInstance(device).VERTEXFINISHINGBACKGROUNDCOLOR;
	}

	public static Color getVertexFinishedBackgroundColor(Device device) {
		return getInstance(device).VERTEXFINISHEDBACKGROUNDCOLOR;
	}

	public static Color getVertexCancelBackgroundColor(Device device) {
		return getInstance(device).VERTEXCANCELBACKGROUNDCOLOR;
	}

	public static Color getVertexFailedBackgroundColor(Device device) {
		return getInstance(device).VERTEXFAILEDBACKGROUNDCOLOR;
	}

	public static Color getVertexDefaultBackgroundColor(Device device) {
		return getInstance(device).VERTEXDEFAULTBACKGROUNDCOLOR;
	}

	public static Color getVertexRunningBackgroundColor(Device device) {
		return getInstance(device).VERTEXRUNNINGBACKGROUNDCOLOR;
	}

	public static Color getVertexReplayingBackgroundColor(Device device) {
		return getInstance(device).VERTEXREPLAYINGBACKGROUNDCOLOR;
	}

	public static Color getGraphBackgroundColor(Device device) {
		return getInstance(device).GRAPHBACKGROUNDCOLOR;
	}

	public static Color getGateDefaultBorderColor(Device device) {
		return getInstance(device).GATEDEFAULTBORDERCOLOR;
	}

	public static Color getGateDefaultBackgroundColor(Device device) {
		return getInstance(device).GATEDEFAULTBACKGROUNDCOLOR;
	}

	public static Color getGateRunningBorderColor(Device device) {
		return getInstance(device).GATERUNNINGBORDERCOLOR;
	}

	public static Color getGateRunningBackgroundColor(Device device) {
		return getInstance(device).GATERUNNINGBACKGROUNDCOLOR;
	}

	public static Color getGateReplayingBorderColor(Device device) {
		return getInstance(device).GATEREPLAYINGBORDERCOLOR;
	}

	public static Color getGateReplayingBackgroundColor(Device device) {
		return getInstance(device).GATEREPLAYINGBACKGROUNDCOLOR;
	}

	public static Color getGateFinishingBorderColor(Device device) {
		return getInstance(device).GATEFINISHINGBORDERCOLOR;
	}

	public static Color getGateFinishingBackgroundColor(Device device) {
		return getInstance(device).GATEFINISHINGBACKGROUNDCOLOR;
	}

	public static Color getGateFinishedBorderColor(Device device) {
		return getInstance(device).GATEFINISHEDBORDERCOLOR;
	}

	public static Color getGateFinishedBackgroundColor(Device device) {
		return getInstance(device).GATEFINISHEDBACKGROUNDCOLOR;
	}

	public static Color getGateCancelBorderColor(Device device) {
		return getInstance(device).GATECANCELBORDERCOLOR;
	}

	public static Color getGateCancelBackgroundColor(Device device) {
		return getInstance(device).GATECANCELBACKGROUNDCOLOR;
	}

	public static Color getGateFailedBorderColor(Device device) {
		return getInstance(device).GATEFAILEDBORDERCOLOR;
	}

	public static Color getGateFailedBackgroundColor(Device device) {
		return getInstance(device).GATEFAILEDBACKGROUNDCOLOR;
	}

	public static Color getGroupVertexBackgroundColor(Device device) {
		return getInstance(device).GROUPVERTEXBACKGROUNDCOLOR;
	}

	public static Color getGroupVertexBorderColor(Device device) {
		return getInstance(device).GROUPVERTEXBORDERCOLOR;
	}

	public static Color getStageBackgroundColor(Device device) {
		return getInstance(device).STAGEBACKGROUNDCOLOR;
	}

	public static Color getStageBorderColor(Device device) {
		return getInstance(device).STAGEBORDERCOLOR;
	}

	public static Color getNetworkTopologyBackgroundColor(Device device) {
		return getInstance(device).NETWORKTOPOLOGYBACKGROUNDCOLOR;
	}

	public static Color getNetworkNodeBorderColor(Device device) {
		return getInstance(device).NETWORKNODEBORDERCOLOR;
	}

	public static Color getNetworkNodeBackgroundColor(Device device) {
		return getInstance(device).NETWORKNODEBACKGROUNDCOLOR;
	}
}
