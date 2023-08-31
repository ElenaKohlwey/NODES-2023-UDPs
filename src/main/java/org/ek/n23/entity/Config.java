package org.ek.n23.entity;

import java.util.HashSet;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * This class describes the Config node in the database.
 *
 * @author Elena Kohlwey
 */
public class Config {

  private Config() {}

  // Label of the Config node
  public static final Label LABEL = Label.label("Config");

  // Property keys of the Config node
  public static final String ACTION_COUNT_MIN_KEY = "actionCountMin";
  public static final String ACTION_COUNT_MAX_KEY = "actionCountMax";
  public static final String ACTION_DURATION_MIN_KEY = "actionDurationMin";
  public static final String ACTION_DURATION_MAX_KEY = "actionDurationMax";
  public static final String STARTING_NODES_COUNT_MIN_KEY =
    "startingNodesCountMin";
  public static final String STARTING_NODES_COUNT_MAX_KEY =
    "startingNodesCountMax";
  public static final String OUTGOING_RELATIONS_COUNT_MIN_KEY =
    "outgoingRelationsCountMin";
  public static final String OUTGOING_RELATIONS_COUNT_MAX_KEY =
    "outgoingRelationsCountMax";

  public static final String VALIDATION_PASSED =
    "The given ConfigNode is valid.";
  public static final String VALIDATION_FAILED =
    "The given ConfigNode is not valid.";

  // region getters

  public static long actionCountMin(Node configNode) {
    return (long) configNode.getProperty(ACTION_COUNT_MIN_KEY, Long.MIN_VALUE);
  }

  public static long actionCountMax(Node configNode) {
    return (long) configNode.getProperty(ACTION_COUNT_MAX_KEY, Long.MIN_VALUE);
  }

  public static long actionDurationMin(Node configNode) {
    return (long) configNode.getProperty(
      ACTION_DURATION_MIN_KEY,
      Long.MIN_VALUE
    );
  }

  public static long actionDurationMax(Node configNode) {
    return (long) configNode.getProperty(
      ACTION_DURATION_MAX_KEY,
      Long.MIN_VALUE
    );
  }

  public static long startingNodesCountMin(Node configNode) {
    return (long) configNode.getProperty(
      STARTING_NODES_COUNT_MIN_KEY,
      Long.MIN_VALUE
    );
  }

  public static long startingNodesCountMax(Node configNode) {
    return (long) configNode.getProperty(
      STARTING_NODES_COUNT_MAX_KEY,
      Long.MIN_VALUE
    );
  }

  public static long outgoingRelationsCountMin(Node configNode) {
    return (long) configNode.getProperty(
      OUTGOING_RELATIONS_COUNT_MIN_KEY,
      Long.MIN_VALUE
    );
  }

  public static long outgoingRelationsCountMax(Node configNode) {
    return (long) configNode.getProperty(
      OUTGOING_RELATIONS_COUNT_MAX_KEY,
      Long.MIN_VALUE
    );
  }

  // endregion

  // region setters

  public static void setActionCountMin(Node configNode, long actionCountMin) {
    configNode.setProperty(ACTION_COUNT_MIN_KEY, actionCountMin);
  }

  public static void setActionCountMax(Node configNode, long actionCountMax) {
    configNode.setProperty(ACTION_COUNT_MAX_KEY, actionCountMax);
  }

  public static void setActionDurationMin(
    Node configNode,
    long actionDurationMin
  ) {
    configNode.setProperty(ACTION_DURATION_MIN_KEY, actionDurationMin);
  }

  public static void setActionDurationMax(
    Node configNode,
    long actionDurationMax
  ) {
    configNode.setProperty(ACTION_DURATION_MAX_KEY, actionDurationMax);
  }

  public static void setStartingNodesCountMin(
    Node configNode,
    long startingNodesCountMin
  ) {
    configNode.setProperty(STARTING_NODES_COUNT_MIN_KEY, startingNodesCountMin);
  }

  public static void setStartingNodesCountMax(
    Node configNode,
    long startingNodesCountMax
  ) {
    configNode.setProperty(STARTING_NODES_COUNT_MAX_KEY, startingNodesCountMax);
  }

  public static void setOutgoingRelationsCountMin(
    Node configNode,
    long outgoingRelationsCountMin
  ) {
    configNode.setProperty(
      OUTGOING_RELATIONS_COUNT_MIN_KEY,
      outgoingRelationsCountMin
    );
  }

  public static void setOutgoingRelationsCountMax(
    Node configNode,
    long outgoingRelationsCountMax
  ) {
    configNode.setProperty(
      OUTGOING_RELATIONS_COUNT_MAX_KEY,
      outgoingRelationsCountMax
    );
  }

  // endregion

  public static String validate(Node configNode) {
    HashSet<String> existenceViolations = new HashSet<>();
    HashSet<String[]> valueViolations = new HashSet<>();

    HashSet<String[]> propertyKeys = new HashSet<>();
    propertyKeys.add(
      new String[] { ACTION_COUNT_MIN_KEY, ACTION_COUNT_MAX_KEY }
    );
    propertyKeys.add(
      new String[] { ACTION_DURATION_MIN_KEY, ACTION_DURATION_MAX_KEY }
    );
    propertyKeys.add(
      new String[] {
        STARTING_NODES_COUNT_MIN_KEY,
        STARTING_NODES_COUNT_MAX_KEY,
      }
    );
    propertyKeys.add(
      new String[] {
        OUTGOING_RELATIONS_COUNT_MIN_KEY,
        OUTGOING_RELATIONS_COUNT_MAX_KEY,
      }
    );

    Object minValue;
    Object maxValue;

    for (String[] minMaxPair : propertyKeys) {
      String minKey = minMaxPair[0];
      String maxKey = minMaxPair[1];

      try {
        minValue = configNode.getProperty(minKey);
      } catch (Exception e) {
        existenceViolations.add(minKey);
        minValue = null;
      }

      try {
        maxValue = configNode.getProperty(maxKey);
      } catch (Exception e) {
        existenceViolations.add(maxKey);
        maxValue = null;
      }

      if (
        minValue != null &&
        maxValue != null &&
        (long) minValue > (long) maxValue
      ) {
        valueViolations.add(minMaxPair);
      }
    }

    if (existenceViolations.isEmpty() && valueViolations.isEmpty()) {
      return VALIDATION_PASSED;
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(VALIDATION_FAILED);
      sb.append("\n");

      if (!existenceViolations.isEmpty()) {
        sb.append("The following properties are not set:\n");
        for (String propertyKey : existenceViolations) {
          sb.append(propertyKey);
          sb.append("\n");
        }
      }

      if (!valueViolations.isEmpty()) {
        sb.append("The following properties do not satisfy min <= max:\n");
        for (String[] minMaxPair : valueViolations) {
          sb.append("It is ");
          sb.append(minMaxPair[0]);
          sb.append(" > ");
          sb.append(minMaxPair[1]);
          sb.append(".\n");
        }
      }

      return sb.toString();
    }
  }
}
