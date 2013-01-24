package eu.stratosphere.pact.runtime.iterative.playing.dsgd;

import java.util.Random;

public class BiasedMatrixFactorization {

  /** learning rate (step size) */
  private final double learningRate = 0.01;
  /** regularization param. */
  private final double reg = 0.01;
  /** Standard deviation for random initialization of features */
  private final double randomNoise = 0.1;

  private final int numFeatures = 3;

  private final Random random = new Random(0xbadc0ffe);

  private double biasLearningRate = 0.5;

  private double biasReg = 0.1;




  private static final int FEATURE_OFFSET = 3;
  /** place in user vector where the bias is stored */
  private static final int USER_BIAS_INDEX = 1;
  /** place in item vector where the bias is stored */
  private static final int ITEM_BIAS_INDEX = 2;

  public double[] initializeUserFeatures(double globalAverage) {
    double[] userFeatures = new double[numFeatures];
    userFeatures[0] = globalAverage;
    userFeatures[USER_BIAS_INDEX] = 0;
    userFeatures[ITEM_BIAS_INDEX] = 1;
    for (int feature = FEATURE_OFFSET; feature < numFeatures; feature++) {
      userFeatures[feature] = random.nextGaussian() * randomNoise;
    }
    return userFeatures;
  }

  public double[] initializeItemFeatures(double globalAverage) {
    double[] itemFeatures = new double[numFeatures];
    itemFeatures[0] = globalAverage;
    itemFeatures[USER_BIAS_INDEX] = 1;
    itemFeatures[ITEM_BIAS_INDEX] = 0;
    for (int feature = FEATURE_OFFSET; feature < numFeatures; feature++) {
      itemFeatures[feature] = random.nextGaussian() * randomNoise;
    }
    return itemFeatures;
  }


  public void updateParameters(double[] userFeatures, double[] itemFeatures, float rating, double learningRate) {

    double prediction = predictRating(userFeatures, itemFeatures);
    double err = rating - prediction;

    // adjust user bias
    userFeatures[USER_BIAS_INDEX] +=
        biasLearningRate * learningRate * (err - biasReg * reg * userFeatures[USER_BIAS_INDEX]);

    // adjust item bias
    itemFeatures[ITEM_BIAS_INDEX] +=
        biasLearningRate * learningRate * (err - biasReg * reg * itemFeatures[ITEM_BIAS_INDEX]);

    // adjust features
    for (int feature = FEATURE_OFFSET; feature < numFeatures; feature++) {
      double userFeature = userFeatures[feature];
      double itemFeature = itemFeatures[feature];

      double deltaUserFeature = err * itemFeature - reg * userFeature;
      userFeatures[feature] += learningRate * deltaUserFeature;

      double deltaItemFeature = err * userFeature - reg * itemFeature;
      itemFeatures[feature] += learningRate * deltaItemFeature;
    }
  }

  private double predictRating(double[] userFeatures, double[] itemFeatures) {
    double sum = 0;
    for (int feature = 0; feature < numFeatures; feature++) {
      sum += userFeatures[feature] * itemFeatures[feature];
    }
    return sum;
  }
}
