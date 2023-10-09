package org.ek.n23;

import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.ek.n23.entity.Action;
import org.ek.n23.entity.Config;
import org.ek.n23.entity.Precedes;
import org.ek.n23.utility.ConfigObjectMap;
import org.ek.n23.utility.NodeComparer;
import org.ek.n23.utility.RandomNumbers;
import org.ek.n23.utility.Summary;
import org.neo4j.graphdb.Node;
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
public class ProcedureGraphCreation {

  @Context
  public Transaction tx;

  protected static final String NO_SUCH_CONFIG =
    "There is no config object with that name";

  private static Summary summary = new Summary();

  /* ProcedureName exists to seperate the names
   * of the procedures from other static
   * string used for error messages and the like
   * Not a necessary seperation, just keeping things tidy
   */
  public static class ProcedureName {

    private ProcedureName() {}

    public static final String GENERATE_GRAPH_BY_SEED_AND_NODE =
      "org.ek.n23.generateGraphBySeedAndNode";

    public static final String GENERATE_GRAPH_BY_SEED_AND_CONFIG =
      "org.ek.n23.generateGraphBySeedAndConfig";

    public static final String GENERATE_CONFIG_NODE =
      "org.ek.n23.generateConfigNode";

    public static final String SHOW_SUMMARY = "org.ek.n23.showSummary";
  }

  /** This function generates the Action graph.
   * It is the main function in this class which is called
   * by the different procedures.
   */
  private Stream<Summary> generateGraph(long seed, ConfigObject config) {
    Random random = new Random(seed);

    summary.clear();

    // create Action nodes and return them for further use
    TreeSet<Node> actionNodes = createActionNodes(random, config);

    // connect Action nodes and return startingNodes for further use
    TreeSet<Node> startingNodes = connectActionNodes(
      random,
      config,
      actionNodes
    );

    // create additional Action nodes for start and end with relationships
    createAdditionalStartAndEndNodes(startingNodes, actionNodes);

    return Stream.of(summary);
  }

  /** This procedure generates an Action graph
   * by receiving a seed value and a configNode from Neo4j.
   */
  @Procedure(
    mode = Mode.WRITE,
    name = ProcedureName.GENERATE_GRAPH_BY_SEED_AND_NODE
  )
  @Description("Create a graph by seed and config node")
  public Stream<Summary> generateGraphBySeedAndNode(
    @Name("Seed") long seed,
    @Name("ConfigNode") Node configNode
  ) {
    ConfigObject config = new ConfigObject(configNode);
    return generateGraph(seed, config);
  }

  /** This procedure generates an Action graph
   * by receiving a seed value and a configObject name from Neo4j.
   */
  @Procedure(
    mode = Mode.WRITE,
    name = ProcedureName.GENERATE_GRAPH_BY_SEED_AND_CONFIG
  )
  @Description("Create a graph by seed and named config")
  public Stream<Summary> generateGraphBySeedAndConfig(
    @Name("Seed") long seed,
    @Name("ConfigName") String configName
  ) {
    ConfigObject config = ConfigObject.getConfig(configName);
    if (config == null) {
      throw new IllegalArgumentException(NO_SUCH_CONFIG);
    }
    return generateGraph(seed, config);
  }

  /** This procedure creates a Config Node in the database
   * with all necessary properties and some default values.
   */
  @Procedure(mode = Mode.WRITE, name = ProcedureName.GENERATE_CONFIG_NODE)
  @Description("Create a config node")
  public Stream<ConfigObjectMap> generateConfigNode() {
    ConfigObject conf = ConfigObject.SampleConfig;
    Node configNode = tx.createNode(Config.LABEL);
    conf.write2Node(configNode);
    return Stream.of(new ConfigObjectMap(configNode));
  }

  /** This procedure returns the summary of the latest graph creation */
  @Procedure(mode = Mode.WRITE, name = ProcedureName.SHOW_SUMMARY)
  @Description("Show summary of last graph creation")
  public Stream<Summary> showSummary() {
    return Stream.of(summary);
  }

  /** This private function generates Action nodes of the provided
   * number and return a HashSet containning them all
   */
  private TreeSet<Node> createActionNodes(Random random, ConfigObject config) {
    TreeSet<Node> nodes = new TreeSet<>(new NodeComparer());
    String name;
    long duration;

    long minNumberActions = config.actionCount().min();
    long maxNumberActions = config.actionCount().max();
    long minActionDuration = config.actionDuration().min();
    long maxActionDuration = config.actionDuration().max();

    long numberNodes = RandomNumbers.randomNumber(
      random,
      minNumberActions,
      maxNumberActions
    );

    // Create the provided number of Action nodes
    for (int i = 1; i <= numberNodes; i++) {
      // determine properties of node
      name = Action.transformToNodeName(i);
      duration =
        RandomNumbers.randomNumber(
          random,
          minActionDuration,
          maxActionDuration
        );

      // create node
      Node newNode = Action.createNode(tx, name, duration);

      // add to collection
      nodes.add(newNode);
    }

    // add info about created Action nodes to summary
    summary.addNodeInfo(
      Action.LABEL_NAME + " (without start and end node)",
      nodes.size()
    );

    return nodes;
  }

  /** This private function creates edges between the Action nodes
   * and returns a HashSet containning all starting nodes
   */
  private TreeSet<Node> connectActionNodes(
    Random random,
    ConfigObject config,
    TreeSet<Node> actionNodes
  ) {
    long minNumberStartingNodes = config.startingNodesCount().min();
    long maxNumberStartingNodes = config.startingNodesCount().max();
    long minNumberOutgoingRels = config.outgoingRelationsCount().min();
    long maxNumberOutgoingRels = config.outgoingRelationsCount().max();

    TreeSet<Node> currentNodes = new TreeSet<>(new NodeComparer());
    TreeSet<Node> unvisitedNodes = new TreeSet<>(new NodeComparer());
    TreeSet<Node> visitedNodes = new TreeSet<>(new NodeComparer());
    TreeSet<Node> startingNodes = new TreeSet<>(new NodeComparer());
    TreeSet<Node> actionNodesWithoutStartingNodes = new TreeSet<>(
      new NodeComparer()
    );

    int relationshipCounter = 0;

    // add all action nodes to unvisited nodes
    unvisitedNodes.addAll(actionNodes);

    // Find starting nodes
    long numberStartingNodes = RandomNumbers.randomNumber(
      random,
      minNumberStartingNodes,
      maxNumberStartingNodes
    );
    for (int i = 1; i <= numberStartingNodes; i++) {
      currentNodes.add(RandomNumbers.getRandomElement(random, actionNodes));
    }

    // all starting nodes are visited by default
    startingNodes.addAll(currentNodes);
    visitedNodes.addAll(currentNodes);
    unvisitedNodes.removeAll(currentNodes);
    actionNodesWithoutStartingNodes.addAll(actionNodes);
    actionNodesWithoutStartingNodes.removeAll(currentNodes);

    // create relationships
    while (!unvisitedNodes.isEmpty()) {
      /* take the next node from the currentNodes list
       * to attach outgoing relationships to it */
      Node currentNode;
      if (!currentNodes.isEmpty()) {
        currentNode = RandomNumbers.getRandomElement(random, currentNodes);
        currentNodes.remove(currentNode);
      } else {
        currentNode = RandomNumbers.getRandomElement(random, visitedNodes);
        Node toNode = RandomNumbers.getRandomElement(random, unvisitedNodes);
        Precedes.createRelationship(currentNode, toNode);
        relationshipCounter++;
        unvisitedNodes.remove(toNode);
        currentNodes.add(toNode);
        continue;
      }

      // create relationships from currentNode
      TreeSet<Node> connectedToNodes = createPrecedesRelationships(
        random,
        currentNode,
        actionNodesWithoutStartingNodes,
        minNumberOutgoingRels,
        maxNumberOutgoingRels
      );

      // add the newly created relationships to the counter
      relationshipCounter += connectedToNodes.size();

      // check all nodes that now have new incoming relationships
      for (Node node : connectedToNodes) {
        /* if the node did not have any incoming relationships
         * before, it has now */
        if (unvisitedNodes.contains(node)) {
          unvisitedNodes.remove(node);
        }

        /* if the node has not received outgoing rels yet
         * and does not stand in line for getting any,
         * add it to the currentNodes list*/
        if (!visitedNodes.contains(node) && !currentNodes.contains(node)) {
          currentNodes.add(node);
        }
      }

      /* after adding outgoing relationships to currentNode
       * put it into the visitedNodes list so that
       * it is not dealt with again */
      visitedNodes.add(currentNode);
    }

    /* add info about number of starting Action nodes
     * and number of created PRECEDES relationships to summary */
    summary.addOtherInfo("Starting Action Nodes", startingNodes.size());
    summary.addRelationshipInfo(Precedes.TYPE_NAME, relationshipCounter);

    return startingNodes;
  }

  /** This private function creates a start and an
   * end Action node with duration 0.
   * All nodes without a predecessor receive an
   * incoming PRECEDES relationship from the start node.
   * All nodes without a successor receive an
   * outgoing PRECEDES relationship to the end node.
   */
  private void createAdditionalStartAndEndNodes(
    TreeSet<Node> startingNodes,
    TreeSet<Node> actionNodes
  ) {
    String name;

    // Create start node
    name = Action.transformToNodeName(0);
    Node startNode = Action.createEndNode(tx, name);

    // Create end node
    name = Action.transformToNodeName(actionNodes.size() + 1);
    Node endNode = Action.createEndNode(tx, name);

    // connect start to all starting nodes
    for (Node startingNode : startingNodes) {
      Precedes.createRelationship(startNode, startingNode);
    }

    // find all ending nodes and connect them to endNode
    for (Node node : actionNodes) {
      if (!Action.hasSuccessors(node)) {
        Precedes.createRelationship(node, endNode);
      }
    }
  }

  /** This private function receives a node and the Set of all nodes and
   * a maximum number of relationships to create to random nodes from the
   * Set and returns the nodes that it has connected to.
   */
  private TreeSet<Node> createPrecedesRelationships(
    Random random,
    Node currentNode,
    TreeSet<Node> nodes,
    long minNumberRels,
    long maxNumberRels
  ) {
    /* get a random number between 1 and the maximum number of
     * outoing relationships to attach to currentNode */
    long numberOutgoingRels = RandomNumbers.randomNumber(
      random,
      minNumberRels,
      maxNumberRels
    );

    // create return set
    TreeSet<Node> newSuccessors = new TreeSet<>(new NodeComparer());

    /* create a set of nodes without the predecessors
     * and direct successors to find possible successors */
    TreeSet<Node> possibleSuccessorNodes = new TreeSet<>(new NodeComparer());
    possibleSuccessorNodes.addAll(nodes);

    /* get all predecessors of the currentNode and
     * remove them from possibleSuccessorNodes */
    Action
      .getAllPredecessors(currentNode)
      .forEach(possibleSuccessorNodes::remove);

    /* get direct successors of the currentNode and
     * remove them from possibleSuccessorNodes */
    Action.getSuccessorsOf(currentNode).forEach(possibleSuccessorNodes::remove);

    // create relationships
    for (int i = 0; i < numberOutgoingRels; i++) {
      if (possibleSuccessorNodes.isEmpty()) {
        break;
      } else {
        Node toNode = RandomNumbers.getRandomElement(
          random,
          possibleSuccessorNodes
        );

        Precedes.createRelationship(currentNode, toNode);
        possibleSuccessorNodes.remove(toNode);
        newSuccessors.add(toNode);
      }
    }

    return newSuccessors;
  }
}
