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
    public static final String BACKWARD_PATH = "org.ek.n23.backwardPath";
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
    /* Set earliest start and earliest finish on startNode */
    Action.setEarliestStart(startNode, 0l);
    Action.setEarliestFinish(startNode, 0l);

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
   * This procedure iterates through the graph from End to Start
   * calculating the latest start and latest finish times.
   * Cp. https://www.pmcalculators.com/how-to-calculate-the-critical-path/,
   * Example 1, Backward Path.
   */
  @Procedure(mode = Mode.WRITE, name = ProcedureName.BACKWARD_PATH)
  @Description("Executes a backward path calculation.")
  public void backwardPath(@Name("Finish node") Node endNode) {
    /* Set latest start and latest finish on endNode */
    long startEndNode = Action.getEarliestStart(endNode);
    Action.setLatestStart(endNode, startEndNode);
    Action.setLatestFinish(endNode, startEndNode);

    /* create queue that contains the current nodes by
     * fetching all relationships that are incoming into endNode,
     * fetching all the start nodes of all relationships and collecting
     * them into currentNodes.
     */
    ArrayDeque<Node> currentNodes = endNode
      .getRelationships(Direction.INCOMING, Precedes.PRECEDES_TYPE)
      .stream()
      .flatMap(rel -> Stream.of(rel.getStartNode()))
      .collect(Collectors.toCollection(ArrayDeque::new));

    /* set the latestFinish value to endNode's latestStart for all nodes in currentNodes */
    currentNodes
      .stream()
      .forEach(node -> Action.setLatestFinish(node, startEndNode));

    /** iteration over all current nodes; fetching of new current nodes
     * until the start node is in the currentNodes set which means that
     * all nodes are worked through */
    while (!currentNodes.isEmpty()) {
      // take the first of the currentNodes
      Node currentNode = currentNodes.poll();

      // set the "latestStart" value on currentNode
      setLatestStart(currentNode);

      /* add all predecessor nodes of currentNode the successors
       * of which have already been calculated through to
       * currentNodes */
      currentNodes.addAll(getReadyPredecessorNodes(currentNode));
    }
  }

  // region forwardPath helper methods

  /**
   * This private method adds node's "earliestStart" and "duration" properties
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
   * This private method checks all successor nodes
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
   * This private method looks at all predecessors of node.
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
       * of the predecessor nodes of node. The return command
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
     * of node to be maxEarliestFinish and return true so that
     * node can be added to the currentNodes queue
     * higher up in the call stack.
     */
    if (!incomingRelsIt.hasNext()) {
      Action.setEarliestStart(node, maxEarliestFinish);
    }
    return true;
  }

  // endregion

  // region backwardPath helper methods

  /**
   * This private method subtracts node's "duration" from its "latestFinish" property
   * to obtain its "latestStart" property. The property is written on the node if
   * "latestFinish" and "duration" properties both exist on node.
   * @param node: the node that the "latestStart" property should be calculated for
   */
  private void setLatestStart(Node node) {
    // fetch "latestFinish" property of node
    long nodeLatestFinish = Action.getLatestFinish(node);

    // fetch "duration" property of node
    long nodeDuration = Action.getDuration(node);

    /* subtract "duration" from "latestFinish" to obtain "latestStart"
     * and set "latestStart" property on currentNode
     */
    Action.setLatestStart(node, nodeLatestFinish - nodeDuration);
  }

  // endregion

  /**
   * This private method checks all predecessor nodes
   * of node to see whether their "LatestFinish" property can already
   * be calculated, i.e. all of their successors have already
   * a calculated "latestStart" value.
   * @param node: node whose predecessors need to be checked
   * @return set of all ready predecessor nodes
   */
  private HashSet<Node> getReadyPredecessorNodes(Node node) {
    /* initialize set of ready predecessor nodes that will be
     * returned by this method. */
    HashSet<Node> readyPredecessorNodes = new HashSet<>();

    /* fetch all incoming relationships of node.
     * Test whether all successors of the predecessors of node
     * have already been calculated through and if so, add the predecessor
     * to readyPredecessorNodes. */
    node
      .getRelationships(Direction.INCOMING, Precedes.PRECEDES_TYPE)
      .stream()
      .forEach(rel -> {
        if (setLatestFinish(rel.getStartNode())) {
          readyPredecessorNodes.add(rel.getStartNode());
        }
      });

    // return the list of all successor nodes of node that are ready.
    return readyPredecessorNodes;
  }

  /**
   * This private method looks at all successors of node.
   * If all of them have "latestStart" values, their minimum yields the
   * node's "latestFinish" value which is set. If one node is found that
   * does not have a "latestStart" value yet, the search is terminated
   * and this method returns "false".
   * @param node: node whose "latestFinish" value is to be calculated.
   * @return true if the "latestFinish" property could be calculated
   * and false otherwise.
   */
  private boolean setLatestFinish(Node node) {
    // fetch all outgoing relationships from node
    Iterator<Relationship> outgoingRelsIt = node
      .getRelationships(Direction.OUTGOING, Precedes.PRECEDES_TYPE)
      .iterator();

    // initialize a maximum value for the "latestStart" values of Long.MAX_VALUE
    long minLatestStart = Long.MAX_VALUE;

    /* test if all start nodes of the incomingRels have already been processed,
     * i.e. whether the property "latestStart" already exists on them */
    while (outgoingRelsIt.hasNext()) {
      // get the end node of next outgoing relationship
      Node successorNode = outgoingRelsIt.next().getEndNode();

      /* fetch the "latestStart" property of successorNode.
       * If the property does not exist that means that successorNode has
       * not yet been processed. Hence, we can stop checking on the rest
       * of the successor nodes of node. The return command
       * in the catch part will quit this method and report back about it.*/
      long successorNodeLatestStart;
      try {
        successorNodeLatestStart = Action.getLatestStart(successorNode);
      } catch (NotFoundException e) {
        return false;
      }

      /* if the property could be fetched, we need to continue finding the minimum
       * of all successor node "latestStart" properties. Hence, if the newly
       * fetched successorNodeLatestStart value is lower than the current minimum,
       * we need to update minLatestStart */
      if (successorNodeLatestStart < minLatestStart) {
        minLatestStart = successorNodeLatestStart;
      }
    }
    /* if every successorNode had a "latestStart" value, we found the
     * minimum of them in minLatestStart. Hence, there are no
     * additional checks needed and we can set the "latestFinish" property
     * of node to be minLatestStart and return true so that
     * node can be added to the currentNodes queue
     * higher up in the call stack.
     */
    if (!outgoingRelsIt.hasNext()) {
      Action.setLatestFinish(node, minLatestStart);
    }
    return true;
  }
}
