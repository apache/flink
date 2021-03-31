/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.operator.common.dataproc;

import org.apache.commons.math3.complex.Complex;

/**
 * Fast Fourier Transformation(FFT).
 * Provides 2 algorithms:
 * 1. Cooley-Tukey algorithm, high performance, but only supports length of power-of-2.
 * 2. Chirp-Z algorithm, can perform FFT with any length.
 */
public class FFT {

	private static final double INVERSE_LOG_2 = 1.0 / Math.log(2);

	/**
	 * Helper for root of unity. Returns the group for power "length".
	 */
	public static Complex[] getOmega(int length) {
		Complex[] omega = new Complex[length];
		Complex unit = new Complex(Math.cos(2 * Math.PI / length),
			Math.sin(2 * Math.PI / length));
		omega[0] = new Complex(1, 0);
		omega[1] = unit;
		for (int index = 2; index < length; index++) {
			omega[index] = omega[index - 1].multiply(unit);
		}
		return omega;
	}

	/**
	 * Cooley-Tukey algorithm, can perform fft for any composite base.
	 * Specifically, it can perform power-of-2 base fft(with some modification to make it an in-place algorithm).
	 *
	 * <p>See reference for more details
	 *
	 * <p><ul>
	 *   <li>"An algorithm for the machine calculation of complex Fourier series", JW Cooley, JW Tukey, 1965
	 *   for a rough reference.
	 *   <li>Detail of radix-2 in-place Cooley-Tukey algorithm can be found in many places, e.g. CLRS textbook.
	 *   </ul>
	 * </p>
	 */
	public static Complex[] fftRadix2CooleyTukey(Complex[] input, boolean inverse, Complex[] omega) {

		//1. length
		int length = input.length;
		int logl = (int) (Math.log(length + 0.01) * INVERSE_LOG_2);

		//notice: only support power of 2
		//fftChirpZ support other lengths
		if ((1 << logl) != length) {
			throw new IllegalArgumentException("Radix-2 Cooley-Tukey only supports lengths of power-of-2.");
		}

		//2. copy data
		Complex[] inPlaceFFT = new Complex[length];
		for (int index = 0; index < length; index++) {
			inPlaceFFT[index] = new Complex(input[index].getReal(), input[index].getImaginary());
		}

		//3. bit reverse
		int[] reverse = new int[length];
		for (int index = 0; index < length; index++) {
			int t = 0;
			for (int pos = 0; pos < logl; pos++) {
				if ((index & (1 << pos)) != 0) {
					t |= (1 << (logl - pos - 1));
				}
			}
			reverse[index] = t;
		}

		//4. reverse the input
		for (int index = 0; index < length; index++) {
			if (index < reverse[index]) {
				Complex t = inPlaceFFT[index];
				inPlaceFFT[index] = inPlaceFFT[reverse[index]];
				inPlaceFFT[reverse[index]] = t;
			}
		}

		//5. perform in-place fft
		if (inverse) {
			//inverse fft
			for (int len = 2; len <= length; len *= 2) {
				int mid = len / 2;
				for (int step = 0; step < length; step += len) {
					for (int index = 0; index < mid; index++) {
						Complex t = omega[length / len * index]
							.multiply(inPlaceFFT[step + mid + index]);
						inPlaceFFT[step + mid + index] = inPlaceFFT[step + index].subtract(t);
						inPlaceFFT[step + index] = inPlaceFFT[step + index].add(t);
					}
				}
			}

			for (int index = 0; index < length; index++) {
				inPlaceFFT[index] = inPlaceFFT[index].divide(length);
			}
		} else {
			//forward fft
			for (int len = 2; len <= length; len *= 2) {
				int mid = len / 2;
				for (int step = 0; step < length; step += len) {
					for (int index = 0; index < mid; index++) {
						Complex t = omega[length / len * index].conjugate()
							.multiply(inPlaceFFT[step + mid + index]);
						inPlaceFFT[step + mid + index] = inPlaceFFT[step + index].subtract(t);
						inPlaceFFT[step + index] = inPlaceFFT[step + index].add(t);
					}
				}
			}
		}

		return inPlaceFFT;

	}

	/**
	 * Chirp-Z algorithm, also called Bluestein algorithm,
	 * can perform fft with any base.
	 * It use convolution and "chirp-z", and the convolution can be performed by a power-of-2 base fft.
	 *
	 * <p>See reference for more details
	 *
	 * <p><ul>
	 *   <li>"The chirp z-transform algorithm", L Rabiner, RW Schafer, C Rader, 1969
	 *   </ul>
	 * </p>
	 **/
	public static Complex[] fftChirpZ(Complex[] input, boolean inverse, Complex[] omega, Complex[] omega2) {

		//1. length
		int length = input.length;
		int logl = (int) (Math.log(length + 0.01) * INVERSE_LOG_2);
		if ((1 << logl) == length) {
			throw new IllegalArgumentException(
				"Chirp-Z is not efficient for lengths of power-of-2. Use Radix-2 Cooley-Tukey instead.");
		}

		int nextLength = 1 << (logl + 2);

		//2. copy data & construct chirps
		Complex[] inputSet = new Complex[nextLength];
		Complex[] convChirpSet = new Complex[nextLength];
		if (inverse) {
			for (int index = 0; index < length; index++) {
				Complex curChirp = omega2[(index * index) % (2 * length)];
				inputSet[index] = input[index].multiply(curChirp);
				convChirpSet[index] = curChirp.conjugate();
			}
		} else {
			for (int index = 0; index < length; index++) {
				Complex curChirp = omega2[(index * index) % (2 * length)];
				inputSet[index] = input[index].multiply(curChirp.conjugate());
				convChirpSet[index] = curChirp;
			}
		}
		for (int index = length; index < nextLength; index++) {
			inputSet[index] = new Complex(0);
			convChirpSet[index] = new Complex(0);
		}
		for (int index = 1; index < length; index++) {
			convChirpSet[nextLength - index] = convChirpSet[index];
		}

		//3. convolution
		Complex[] fftInputSet = fftRadix2CooleyTukey(inputSet, false, omega);
		Complex[] fftConvChirpSet = fftRadix2CooleyTukey(convChirpSet, false, omega);
		for (int index = 0; index < nextLength; index++) {
			fftInputSet[index] = fftInputSet[index].multiply(fftConvChirpSet[index]);
		}
		Complex[] fftConvResult = fftRadix2CooleyTukey(fftInputSet, true, omega);

		//4. modify result
		Complex[] result = new Complex[length];
		if (inverse) {
			for (int index = 0; index < length; index++) {
				Complex curChirp = omega2[(index * index) % (2 * length)];
				result[index] = fftConvResult[index].multiply(curChirp).divide(length);
			}
		} else {
			for (int index = 0; index < length; index++) {
				Complex curChirp = omega2[(index * index) % (2 * length)];
				result[index] = fftConvResult[index].multiply(curChirp.conjugate());
			}
		}

		return result;
	}
}
