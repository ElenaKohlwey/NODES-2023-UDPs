package org.ek.n23.utility;

import java.util.Comparator;
import org.ek.n23.entity.Action;
import org.neo4j.graphdb.Node;

/**
 * This class implements the Comparator interface for Neo4j Node objects.
 * It is used in the procedures to put nodes into a TreeSet.
 * The nodes need to be put into a sorted collection since using
 * unsorted collections (like hashsets) leads to non-deterministic
 * results.
 *
 * @author Jens Deininger, Elena Kohlwey
 */
public class NodeComparer implements Comparator<Node> {

  @Override
  public int compare(Node n1, Node n2) {
    if (n1 == null && n2 == null) {
      return 0;
    }
    if (n1 == null) {
      return -1;
    }
    if (n2 == null) {
      return 1;
    }
    return ((String) n1.getProperty(Action.NAME_KEY, -1)).compareTo(
        (String) n2.getProperty(Action.NAME_KEY, -1)
      );
  }
}
