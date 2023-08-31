package org.ek.n23.entity;

import java.util.ArrayDeque;
import java.util.SortedSet;
import java.util.TreeSet;
import org.ek.n23.utility.NodeComparer;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

/**
 * This class is a representation of the
 * Action node in the database.
 */
public class Action {

  // Label of the Action node
  public static final String LABEL_NAME = "Action";
  public static final Label LABEL = Label.label(LABEL_NAME);

  // Property keys of the Action node
  public static final String NAME_KEY = "name";
  public static final String DURATION_KEY = "duration";

  // takes a number and returns LABEL_NAME + number as a String
  public static String transformToNodeName(int number) {
    return LABEL_NAME + number;
  }

  /**
   * This method creates a new Action node in the database
   * @param tx: transaction object
   * @param name: name property of new Action node
   * @param duration: duration property of new Action node
   * @return the newly created Action node object
   */
  public static Node createNode(Transaction tx, String name, long duration) {
    // create node in db
    Node newNode = tx.createNode();

    // sets properties
    newNode.setProperty(NAME_KEY, name);
    newNode.setProperty(DURATION_KEY, duration);

    // returns the created node
    return newNode;
  }

  // region getters

  public String getName(Node actionNode) {
    return (String) actionNode.getProperty(NAME_KEY, "");
  }

  public long getDuration(Node actionNode) {
    return (long) actionNode.getProperty(DURATION_KEY, Long.MIN_VALUE);
  }

  // endregion

  public static SortedSet<Node> getSuccessorsOf(Node actionNode) {
    TreeSet<Node> successors = new TreeSet<>(new NodeComparer());

    for (Relationship rel : actionNode.getRelationships(
      Direction.OUTGOING,
      Precedes.PRECEDES_TYPE
    )) {
      successors.add(rel.getEndNode());
    }
    return successors;
  }

  public static boolean hasSuccessors(Node actionNode) {
    return actionNode
      .getRelationships(Direction.OUTGOING, Precedes.PRECEDES_TYPE)
      .iterator()
      .hasNext();
  }

  public static SortedSet<Node> getPredecessorsOf(Node actionNode) {
    TreeSet<Node> predecessors = new TreeSet<>(new NodeComparer());

    for (Relationship rel : actionNode.getRelationships(
      Direction.INCOMING,
      Precedes.PRECEDES_TYPE
    )) {
      predecessors.add(rel.getStartNode());
    }
    return predecessors;
  }

  public static SortedSet<Node> getAllPredecessors(Node actionNode) {
    TreeSet<Node> predecessors = new TreeSet<>(new NodeComparer());

    ArrayDeque<Node> nodesToCheck = new ArrayDeque<>();

    Node currentNode;

    // add input node as first to be checked
    nodesToCheck.add(actionNode);

    // while there are nodes to be checked, keep fetching their predecessors and checking them
    while (!nodesToCheck.isEmpty()) {
      // get first node
      currentNode = nodesToCheck.poll();

      getPredecessorsOf(currentNode)
        .stream()
        .filter(o -> !predecessors.contains(o) && !nodesToCheck.contains(o))
        .forEach(nodesToCheck::add);

      /* when all predecessors of the current Node were added to the nodesToCheck queue, the current Node can be added to the predecessors set*/
      predecessors.add(currentNode);
    }

    return predecessors;
  }
}
