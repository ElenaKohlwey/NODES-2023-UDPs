package org.ek.n23.utility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class contains methods that return random numbers in a range
 * or a random element from some collection using a given Random object.
 *
 * @author Jens Deininger, Elena Kohlwey
 */
public class RandomNumbers {

  private RandomNumbers() {}

  // returns a random integer number between min (inclusive) and max (inclusive)
  public static int randomNumber(Random random, int min, int max) {
    int randomInt = random.nextInt(max - min + 1);
    return randomInt + min;
  }

  // returns a random long number between min (inclusive) and max (inclusive)
  public static long randomNumber(Random random, long min, long max) {
    long randomInt = random.nextLong(max - min + 1l);
    return randomInt + min;
  }

  // returns a random element in a List
  public static <T> T getRandomElement(Random random, List<T> things) {
    return things.get(random.nextInt(things.size()));
  }

  // returns a random element in a SortedSet
  public static <T> T getRandomElement(Random random, SortedSet<T> things) {
    return getRandomElement(random, new ArrayList<>(things));
  }

  // returns a random element in a Collection
  public static <T> T getRandomElement(Random random, Collection<T> things) {
    return getRandomElement(random, new ArrayList<>(things));
  }

  // returns a random element in an Iterable
  public static <T> T getRandomElement(Random random, Iterable<T> things) {
    List<T> list = StreamSupport
      .stream(things.spliterator(), false)
      .collect(Collectors.toList());
    return list.get(random.nextInt(list.size()));
  }
}
