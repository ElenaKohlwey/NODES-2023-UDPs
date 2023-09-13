package org.ek.n23.utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Node;

/**
 * This Object is an Output object for the {@link generateConfigNode} procedure.
 * It contains all property keys and values generated on the configNode.
 *
 * The reason for using two Lists here and not a Map which would seem more straight
 * forward (especially since the Node function getAllProperties() returns a map)
 * is that the Map (no matter whether you use a TreeMap or any other sorted map)
 * will arrive unsorted in the Neo4j output (probably because it is serialized
 * when being streamed and deserialized into a "normal" Map afterwards).
 *
 * You obtain a nicely sorted output by using the following Cypher query:
 * CALL org.ek.n23.generateConfigNode()
 * YIELD keys, values
 * UNWIND range(0,size(keys)-1) AS item
 * RETURN keys[item] AS key, values[item] AS value
 *
 * @author Elena Kohlwey
 */
@SuppressWarnings("java:S1104") // complains about there being public non static non final fields and no accessors. But Neo4j needs those in its wrapper objects
public class ConfigObjectMap {

  public static final String KEYS_NAME = "keys";
  public static final String VALUES_NAME = "values";

  /* The name of these public fields must always be the same as the static Strings above
   * These strings are needed in the cypher to get to the content of the returned values */
  public List<String> keys;
  public List<Long> values;

  public ConfigObjectMap(Node configNode) {
    // output lists
    this.keys = new ArrayList<>();
    this.values = new ArrayList<>();

    // convert map of all properties into the two lists
    configNode
      .getAllProperties()
      .entrySet()
      .stream()
      .forEach(entry -> {
        keys.add(entry.getKey());
        values.add((long) entry.getValue());
      });
  }
}
