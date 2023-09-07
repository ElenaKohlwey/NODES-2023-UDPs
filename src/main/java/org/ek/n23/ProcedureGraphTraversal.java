package org.ek.n23;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.ek.n23.entity.Action;
import org.ek.n23.entity.Precedes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * This class contains from Neo4j callable procedures
 * and some private helper functions for these methods.
 *
 * @author Elena Kohlwey
 */
public class ProcedureGraphTraversal {

  @Context
  public Transaction tx;

  public static class ProcedureName {

    private ProcedureName() {}

    public static final String FORWARD_PATH = "org.ek.n23.forwardPath";
  }

  /**
   * This procedure iterates through the graph from Start to End
   * calculating the earliest start and earliest finish times.
   * Cp. https://www.pmcalculators.com/how-to-calculate-the-critical-path/,
   * Example 1, Forward Path.
   */
  @Procedure(mode = Mode.WRITE, name = ProcedureName.FORWARD_PATH)
  @Description("Executes a forward path calculation.")
  public void forwardPath(@Name("Start node") Node startNode) {
    /* create queue that contains the current nodes by
     * fetching all relationships that are outgoing from startNode,
     * fetching all the end nodes of all relationships and collecting
     * them into currentNodes.
     */
    ArrayDeque<Node> currentNodes = startNode
      .getRelationships(Direction.OUTGOING, Precedes.PRECEDES_TYPE)
      .stream()
      .flatMap(rel -> Stream.of(rel.getEndNode()))
      .collect(Collectors.toCollection(ArrayDeque::new));

    /* set the earliestStart value to 0 for all nodes in currentNodes */
    currentNodes.stream().forEach(node -> Action.setEarliestStart(node, 0l));

    /** iteration over all current nodes; fetching of new current nodes
     * until the end node is in the currentNodes set which means that
     * all nodes are worked through */
    while (!currentNodes.isEmpty()) {
      // take the first of the currentNodes
      Node currentNode = currentNodes.poll();

      // set the "earliestFinish" value on currentNode
      setEarliestFinish(currentNode);

      /* add all successor nodes of currentNode the predecessors
       * of which have already been calculated through to
       * currentNodes */
      currentNodes.addAll(getReadySuccessorNodes(currentNode));
    }
  }

  /**
   * This private (!) method adds node's "earliestStart" and "duration" properties
   * to obtain its "earliestFinish" property. The property is written on the node if
   * "earliestStart" and "duration" properties both exist on node.
   * @param node: the node that the "earliestFinish" property should be calculated for
   */
  private void setEarliestFinish(Node node) {
    // fetch "earliestStart" property of node
    long nodeEarliestStart = Action.getEarliestStart(node);

    // fetch "duration" property of node
    long nodeDuration = Action.getDuration(node);

    /* add "earliestStart" and "duration" to obtain "earliestFinish"
     * and set "earliestFinish" property on currentNode
     */
    Action.setEarliestFinish(node, nodeEarliestStart + nodeDuration);
  }

  /**
   * This private (!) method checks all successor nodes
   * of node to see whether their "earliestStart" property can already
   * be calculated, i.e. all of their predecessors have already
   * a calculated "earliestFinish" value.
   * @param node: node whose successors need to be checked
   * @return set of all ready successor nodes
   */
  private HashSet<Node> getReadySuccessorNodes(Node node) {
    /* initialize set of ready successor nodes that will be
     * returned by this method. */
    HashSet<Node> readySuccessorNodes = new HashSet<>();

    /* fetch all outgoing relationships of node.
     * Test whether all predecessors of the successors of node
     * have already been calculated through and if so, add the successor
     * to readySuccessorNodes. */
    node
      .getRelationships(Direction.OUTGOING, Precedes.PRECEDES_TYPE)
      .stream()
      .forEach(rel -> {
        if (setEarliestStart(rel.getEndNode())) {
          readySuccessorNodes.add(rel.getEndNode());
        }
      });

    // return the list of all successor nodes of node that are ready.
    return readySuccessorNodes;
  }

  /**
   * This private (!) method looks at all predecessors of node.
   * If all of them have "earliestFinish" values, their maximum yields the
   * node's "earliestStart" value which is set. If one node is found that
   * does not have an "earliestFinish" value yet, the search is terminated
   * and this method returns "false".
   * @param node: node whose "earliestStart" value is to be calculated.
   * @return true if the "earliestStart" property could be calculated
   * and false otherwise.
   */
  private boolean setEarliestStart(Node node) {
    // fetch all incoming relationships into node
    Iterator<Relationship> incomingRelsIt = node
      .getRelationships(Direction.INCOMING, Precedes.PRECEDES_TYPE)
      .iterator();

    // initialize a maximum value for the "earliestFinish" values of 0
    long maxEarliestFinish = 0;

    /* test if all start nodes of the incomingRels have already been processed,
     * i.e. whether the property "earliestFinish" already exists on them */
    while (incomingRelsIt.hasNext()) {
      // get the start node of next incoming relationship
      Node predecessorNode = incomingRelsIt.next().getStartNode();

      /* fetch the "earliestFinish" property of predecessorNode.
       * If the property does not exist that means that predecessorNode has
       * not yet been processed. Hence, we can stop checking on the rest
       * of the predecessor nodes of successorNode. The return command
       * in the catch part will quit this method and report back about it.*/
      long predecessorNodeEarliestFinish;
      try {
        predecessorNodeEarliestFinish =
          Action.getEarliestFinish(predecessorNode);
      } catch (NotFoundException e) {
        return false;
      }

      /* if the property could be fetched, we need to continue finding the maximum
       * of all predecessor node "earliestFinish" properties. Hence, if the newly
       * fetched predecessorNodeEarliestFinish value is higher than the current maximum,
       * we need to update maxEarliestFinish */
      if (predecessorNodeEarliestFinish > maxEarliestFinish) {
        maxEarliestFinish = predecessorNodeEarliestFinish;
      }
    }
    /* if every predecessorNode had an "earliestFinish" value, we found the
     * maximum of them in maxEarliestFinish. Hence, there are no
     * additional checks needed and we can set the "earliestStart" property
     * of successorNode to be maxEarliestFinish and return true so that
     * successorNode can be added to the currentNodes queue
     * higher up in the call stack.
     */
    if (!incomingRelsIt.hasNext()) {
      Action.setEarliestStart(node, maxEarliestFinish);
    }
    return true;
  }
}
