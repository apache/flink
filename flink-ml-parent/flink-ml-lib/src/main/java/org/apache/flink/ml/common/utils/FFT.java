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

package org.apache.flink.ml.common.utils;

import org.apache.commons.math3.complex.Complex;

/**
 * Fast Fourier Transformation(FFT).
 * Provides 2 algorithms:
 * 1. Cooley-Tukey algorithm, high performance, but only supports length of power-of-2.
 * 2. Chirp-Z algorithm, can perform FFT with any length.
 */
public class FFT {
	/**
	 * Helper for root of unity. Returns the group for power "length".
	 */
	public static Complex[] getOmega(int length) throws Exception {
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
	 * See:
	 * "An algorithm for the machine calculation of complex Fourier series", JW Cooley, JW Tukey, 1965
	 * for a rough reference.
	 * Detail of radix-2 in-place Cooley-Tukey algorithm can be found in many places, e.g. CLRS textbook.
	 */
	public static Complex[] fftRadix2CooleyTukey(Complex[] input, Boolean inverse, Complex[] omega) throws Exception {

		//1. length
		int length = input.length;
		int logl = (int) (Math.log(length + 0.01) / Math.log(2));

		//notice: only support power of 2
		//fftChirpZ support other lengths
		if ((1 << logl) != length) {
			throw new RuntimeException("Radix-2 Cooley-Tukey only supports lengths of power-of-2.");
		}

		//2. copy data
		Complex[] inputCopy = new Complex[length];
		for (int index = 0; index < length; index++) {
			inputCopy[index] = new Complex(input[index].getReal(), input[index].getImaginary());
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
				Complex t = inputCopy[index];
				inputCopy[index] = inputCopy[reverse[index]];
				inputCopy[reverse[index]] = t;
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
							.multiply(inputCopy[step + mid + index]);
						inputCopy[step + mid + index] = inputCopy[step + index].subtract(t);
						inputCopy[step + index] = inputCopy[step + index].add(t);
					}
				}
			}

			for (int index = 0; index < length; index++) {
				inputCopy[index] = inputCopy[index].divide(length);
			}
		} else {
			//forward fft
			for (int len = 2; len <= length; len *= 2) {
				int mid = len / 2;
				for (int step = 0; step < length; step += len) {
					for (int index = 0; index < mid; index++) {
						Complex t = omega[length / len * index].conjugate()
							.multiply(inputCopy[step + mid + index]);
						inputCopy[step + mid + index] = inputCopy[step + index].subtract(t);
						inputCopy[step + index] = inputCopy[step + index].add(t);
					}
				}
			}
		}

		return inputCopy;

	}

	/**
	 * Chirp-Z algorithm, also called Bluestein algorithm,
	 * can perform fft with any base.
	 * It use convolution and "chirp-z", and the convolution can be performed by a power-of-2 base fft.
	 * See:
	 * "The chirp z-transform algorithm", L Rabiner, RW Schafer, C Rader, 1969
	 * for details.
	 **/
	public static Complex[] fftChirpZ(Complex[] input, Boolean inverse, Complex[] omega, Complex[] omega2)
		throws Exception {

		//1. length
		int length = input.length;
		int logl = (int) (Math.log(length + 0.01) / Math.log(2));
		if ((1 << logl) == length) {
			throw new RuntimeException(
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
