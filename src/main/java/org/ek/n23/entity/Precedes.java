package org.ek.n23.entity;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * This class is a representation of the
 * PRECEDES relationship in the database.
 */
public class Precedes {

  public static final String TYPE_NAME = "PRECEDES";
  public static final RelationshipType PRECEDES_TYPE = RelationshipType.withName(
    TYPE_NAME
  );

  // private constructor
  private Precedes() {}

  /**
   * This method creates a new PRECEDES relationship in the database
   * @param start: start node of the new relationship
   * @param end: end node of the new relationship
   * @return the new relationship
   */
  public static Relationship createRelationship(Node start, Node end) {
    return start.createRelationshipTo(end, PRECEDES_TYPE);
  }
}
