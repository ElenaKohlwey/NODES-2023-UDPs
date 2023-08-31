package org.ek.n23.utility;

/**
 * This class contains the min and the max of an input value.
 * It is used for the {@link ConfigObject} mappings.
 *
 * @author Jens Deininger
 */
public class MinMax<T extends Number> {

  private static final String MAX_MIN = "max must not be smaller than min";

  private final T mMin;

  private final T mMax;

  public MinMax(T min, T max) {
    if (min == null || max == null) {
      throw new IllegalArgumentException();
    }
    if (min.doubleValue() > max.doubleValue()) {
      throw new IllegalArgumentException(MAX_MIN);
    }
    mMin = min;
    mMax = max;
  }

  public T min() {
    return mMin;
  }

  public T max() {
    return mMax;
  }
}
