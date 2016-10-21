package flink.gelly.school;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
/**
 * This is the skeleton code for the Gellyschool.com Tutorial#1.
 *
 * <p>
 * This program:
 * <ul>
 * <li>reads a list edges
 * <li>creates a graph from the edge data
 * <li>calls Gelly's Connected Components library method on the graph
 * <li>prints the result to stdout
 * </ul>
 *
 */
public class Tutorial1 {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
        * TODO: remove the comments and fill in the "..."
        */
        // Step #1: Load the data in a DataSet
        DataSet<Tuple2<Long, Long>> twitterEdges = env.readCsvFile("./datasets/out.munmun_twitter_social")
        .fieldDelimiter(" ")	// node IDs are separated by spaces
        .ignoreComments("%")	// comments start with "%"
        .types(Long.class,Long.class);	// read the node IDs as Longs

        //        twitterEdges.print();

        // Step #2: Create a Graph and initialize vertex values

//        Graph<Long, Long, NullValue> graph = Graph.fromTuple2DataSet(twitterEdges, new InitVertices(), env);
        Graph<Long, NullValue, NullValue> graph = Graph.fromCsvReader("./datasets/out.munmun_twitter_social", env)
                .fieldDelimiterEdges(" ") // node IDs are separated by spaces
                .ignoreCommentsEdges("%") // comments start with "%"
                .keyType(Long.class); // no vertex or edge values

        /** Step #1: Compute the range of node IDs **/
        Edge<Long, Double> e = new Edge<Long, Double>(1L, 2L, 0.5);

        TemporalEdge<Long,Double,Long> tedge;
        tedge = new TemporalEdge<Long,Double,Long>(1L,2L,0.5,1L,2L);


        DataSet<Tuple5<Long, Long, Double, Long, Long>> testeedges = env.readCsvFile("./datasets/testdata")
                .fieldDelimiter(" ")	// node IDs are separated by spaces
                .ignoreComments("%")	// comments start with "%"
                .types(Long.class,Long.class,Double.class,Long.class,Long.class);
        testeedges.print();


//        DataSet<Tuple1<Long>> vertexIDs;
//        DataSet<Long> vertexlongs = graph.getVertexIds();
//        for (Long vertex : vertexlongs ) {
//
//        }

//        Long mylong;
//        Tuple1<Long> myTuple = new Tuple1<Long> (mylong);


//
//        DataSet<Tuple2<Long,Long>> edgeidset = graph.getEdgeIds();
//
//        DataSet<Tuple2<Long,LongValue>> test = graph.inDegrees();
//        DataSet<Tuple1<Long>> maxInDegreeVertex = graph.getDegrees().maxBy(1).project(0);
//        maxInDegreeVertex.print();
//
//        test.print();
//        long temp1 = graph.numberOfEdges();
//        long temp12 = graph.numberOfVertices();
//        System.out.println(temp1 + " " + temp12);

        //        graph.getEdges().print();
        //        graph.getVertices().print();

        // Step #3: Run Connected Components
        //         int maxinterations = 100;
//                 DataSet<Vertex<Long, Long>> verticesWithComponents = graph.run(new ConnectedComponents<Long,Long, NullValue>(maxinterations));

        // Print the result
        //         verticesWithComponents.print();
        /** get the number of vertices **/
//        long numVertices = graph.numberOfVertices();
//
//
//        long orderedEdges = graph.getEdgeIds().filter(new OrderedEdgesFilter()).count();
//        System.out.println(orderedEdges);
/** compute the average node out-degree **/


//        DataSet<Tuple2<Long, LongValue>> verticesWithDegrees = graph.outDegrees();
//
//        DataSet<Double> avgOutDegree;
//        avgOutDegree = verticesWithDegrees.sum(1).map(new AvgNodeDegreeMapper(numVertices));

        // Get the node IDs in a DataSet<Long> and map it to a DataSet<Tuple1<Long>:
        DataSet<Tuple1<Long>> vertexIDs;
        vertexIDs = graph.getVertexIds().map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                Tuple1<Long> temp = new Tuple1<Long>(value);
                return temp;
            }
        });
        vertexIDs = graph.getVertexIds().map(new TupleWrapperMap() );

//vertexIDs.print();
// Get and print the min node ID
        ReduceOperator<Tuple1<Long>> temp2 = vertexIDs.maxBy(0);
        temp2.print();
// Get and print the max node ID
        vertexIDs.maxBy(0).printOnTaskManager("Max node id");
    }
    private static final class TupleWrapperMap implements MapFunction<Long, Tuple1<Long>> {

//        private Object Tuple;

        @Override
        public Tuple1<Long> map(Long id) {
            Tuple1<Long> temp = new Tuple1<Long>(id);
            return temp;
        }
    }
    private static final class OrderedEdgesFilter implements FilterFunction<Tuple2<Long,Long>> {

        @Override
        public boolean filter(Tuple2<Long, Long> edge) {
            return edge.f0 < edge.f1;
        }
    };
    private static final class AvgNodeDegreeMapper implements MapFunction<Tuple2<Long, LongValue>,Double> {

        private long numberOfVertices;

        public AvgNodeDegreeMapper(long numberOfVertices) {
            this.numberOfVertices = numberOfVertices;
        }

        @Override
        public Double map(Tuple2<Long, LongValue> sumTuple) throws Exception {
//            return (double) (sumTuple.f1 /  numberOfVertices);
            return null;
        }
    }
    //
    // 	User Functions
    //

    /**
     * Initializes the vertex values with the vertex ID
     */
    @SuppressWarnings("serial")
    public static final class InitVertices implements MapFunction<Long, Long> {

        @Override
        public Long map(Long vertexId) {
            return vertexId;
        }
    }
}