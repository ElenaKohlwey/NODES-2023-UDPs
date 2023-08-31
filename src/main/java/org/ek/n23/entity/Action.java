package org.ek.n23.entity;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * This class is a representation of the
 * Action node in the database.
 */
public class Action {

  // Label of the Action node
  public static final Label LABEL = Label.label("Action");

  // Property keys of the Action node
  public static final String NAME_KEY = "name";
  public static final String DURATION_KEY = "duration";

  /**
   * This method creates a new Action node in the database
   * @param tx: transaction object
   * @param name: name property of new Action node
   * @param duration: duration property of new Action node
   * @return the newly created Action node object
   */
  protected Node createAction(Transaction tx, String name, long duration) {
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

}
