package org.ek.n23.utility;

import java.util.HashMap;
import java.util.Map;

/**
 * This Object is an Output object for the {@link generateGraph} procedures.
 * It contains the information about the number of nodes and relationships
 * that have been created by the procedure and some other additional information.
 *
 * You obtain the output by using one of the following Cypher queries:
 * If you are using a ConfigObject to generate a node:
 * CALL org.ek.n23.generateGraphBySeedAndConfig(5,"NODES2023")
 *
 * If you are using an already existing ConfigNode:
 * MATCH (n:ConfigNode)
 * CALL org.ek.n23.generateGraphBySeedAndConfig(5,n)
 * YIELD nodes, relationships, other
 * RETURN nodes, relationships, other
 *
 * @author Elena Kohlwey
 */
@SuppressWarnings("java:S1104") // complains about there being public non static non final fields and no accessors. But Neo4j needs those in its wrapper objects
public class Summary {

  public static final String NODES_MAP = "nodes";

  public static final String RELATIONSHIPS_MAP = "relationships";

  public static final String OTHER_MAP = "other";

  /* The name of these public fields must always be the same as the static Strings above
   * These strings are needed in the cypher to get to the content of the returned values */

  public Map<String, Integer> nodes;
  public Map<String, Integer> relationships;
  public Map<String, Integer> other;

  public Summary() {
    this.nodes = new HashMap<>();
    this.relationships = new HashMap<>();
    this.other = new HashMap<>();
  }

  public void addNodeInfo(String label, int count) {
    this.nodes.put(label, count);
  }

  public void addRelationshipInfo(String label, int count) {
    this.relationships.put(label, count);
  }

  public void addOtherInfo(String label, int count) {
    this.other.put(label, count);
  }

  public void clear() {
    this.nodes.clear();
    this.relationships.clear();
    this.other.clear();
  }
}
