package rsjoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This class implements triangle counting using Reduce-Side Join (RS Join) in MapReduce.
 * The job computes the number of triangles in a graph using a two-phase MapReduce approach.
 */
public class RSJoinTriangleCount extends Configured implements Tool {

    // Logger for logging messages
    private static final Logger log = LogManager.getLogger(RSJoinTriangleCount.class);

    // Threshold to limit the processing of user IDs
    public static int MAX_THRESHOLD = 12500;

    // Enum for maintaining global counters across the job
    public enum TriangleMetrics {
        TRIANGLE_COUNT // Counter to keep track of the number of triangles
    }

    /**
     * Mapper for the first phase: This reads the edges and emits two key-value pairs for each edge.
     * One for the outgoing edge (X -> Y) and one for the incoming edge (Y <- X).
     */
    public static class PathMapper extends Mapper<Object, Text, Text, Text> {

        private final Text userKey = new Text(); // User key for the output

        /**
         * The map method processes each line from the input file (edges.csv) and emits key-value pairs:
         * - For (X, Y), it emits (X, "OUT") and (Y, "IN") if both X and Y are below MAX_THRESHOLD.
         */
        @Override
        public void map(final Object inputKey, final Text inputValue, final Context context) throws IOException, InterruptedException {
            final StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());
            while (tokenizer.hasMoreTokens()) {
                String[] userIDs = tokenizer.nextToken().split(",");

                // Only process user IDs that are below the MAX_THRESHOLD
                if ((Integer.parseInt(userIDs[0]) < MAX_THRESHOLD) && (Integer.parseInt(userIDs[1]) < MAX_THRESHOLD)) {
                    // Emit outgoing edge (X, Y)
                    userKey.set(userIDs[0]);
                    context.write(userKey, new Text(userIDs[0] + "," + userIDs[1] + ",OUT"));

                    // Emit incoming edge (Y, X)
                    userKey.set(userIDs[1]);
                    context.write(userKey, new Text(userIDs[0] + "," + userIDs[1] + ",IN"));
                }
            }
        }
    }

    /**
     * Reducer for the first phase: This collects the incoming and outgoing edges for each user
     * and generates the 2-hop paths, ensuring there are no redundant paths (like X, Y, X).
     */
    public static class PathReducer extends Reducer<Text, Text, Object, Text> {

        /**
         * The reduce method collects incoming and outgoing edges for each user and generates 2-hop paths.
         * It ensures that no redundant paths (like X, Y, X) are emitted.
         */
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            List<String> incoming = new ArrayList<>(); // List to store incoming edges
            List<String> outgoing = new ArrayList<>(); // List to store outgoing edges

            // Separate incoming and outgoing edges for this user
            for (final Text value : values) {
                String[] tokens = value.toString().split(",");
                if (tokens[2].equals("IN")) {
                    incoming.add(tokens[0]); // Add to incoming edges
                } else {
                    outgoing.add(tokens[1]); // Add to outgoing edges
                }
            }

            // Generate 2-hop paths by combining incoming and outgoing edges
            for (int i = 0; i < incoming.size(); i++) {
                for (int j = 0; j < outgoing.size(); j++) {
                    // Ensure we don't create a path like X -> Y -> X
                    if (!incoming.get(i).equals(outgoing.get(j))) {
                        // Emit the 2-hop path
                        context.write(NullWritable.get(), new Text(incoming.get(i) + "," + key.toString() + "," + outgoing.get(j)));
                    }
                }
            }
        }
    }

    /**
     * Mapper for the second phase: This reads the 2-hop paths and emits key-value pairs
     * where the key is the (Z, X) and the value is the full 2-hop path.
     */
    public static class TriangleMapper extends Mapper<Object, Text, Text, Text> {
        private final Text combinedKey = new Text(); // Key for (Z, X)
        private final Text recordValue = new Text(); // The 2-hop path record

        /**
         * The map method processes the output from the first job (2-hop paths).
         * It emits (Z, X) as the key and the full 2-hop path as the value.
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            // Emit (Z, X) as the key and the 2-hop path as the value
            combinedKey.set(new Text(tokens[2] + "," + tokens[0])); // Z, X
            recordValue.set(new Text(value.toString() + ",Path")); // Append "Path" label
            context.write(combinedKey, recordValue);
        }
    }

    /**
     * Mapper for the second phase: This reads the original edge list and emits key-value pairs
     * where the key is the (Z, X) and the value is the original edge.
     */
    public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {

        private final Text record = new Text(); // The edge record

        /**
         * The map method processes the original edge list and emits (Z, X) as the key
         * and the original edge as the value.
         */
        @Override
        protected void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] edgeTokens = value.toString().split(",");
            if (Integer.parseInt(edgeTokens[0]) < MAX_THRESHOLD && Integer.parseInt(edgeTokens[1]) < MAX_THRESHOLD) {
                // Emit the original edge with (Z, X) as the key
                record.set(new Text(value.toString() + ",Edge")); // Append "Edge" label
                context.write(value, record);
            }
        }
    }

    /**
     * Reducer for the second phase: This collects the 2-hop paths and the original edges
     * and checks for the existence of a triangle.
     */
    public static class TriangleReducer extends Reducer<Text, Text, Object, Text> {

        /**
         * The reduce method checks if there is both a 2-hop path and an original edge,
         * indicating the presence of a triangle. It then increments the triangle counter.
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long pathCount = 0; // Count of 2-hop paths
            boolean hasEdge = false; // Flag to check if the original edge exists

            // Iterate over the values to distinguish between paths and edges
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                if (tokens.length == 3) { // Path
                    hasEdge = true;
                } else { // Edge
                    pathCount++;
                }
            }

            // If both a path and an edge exist, it forms a triangle
            if (hasEdge) {
                context.getCounter(TriangleMetrics.TRIANGLE_COUNT).increment(pathCount);
            }
        }
    }

    /**
     * Helper method to run the first MapReduce job (PathJob) that generates the 2-hop paths.
     */
    private int runPathJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration conf = getConf();
        final Job pathJob = Job.getInstance(conf, "PathJob");
        pathJob.setJarByClass(RSJoinTriangleCount.class);

        // Add multiple input paths and set Mapper for the first job
        MultipleInputs.addInputPath(pathJob, new Path(input), TextInputFormat.class, PathMapper.class);
        pathJob.setReducerClass(PathReducer.class);

        pathJob.setMapOutputKeyClass(Text.class);
        pathJob.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(pathJob, new Path(output + "/Temp")); // Temporary output for intermediate results
        return pathJob.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Helper method to run the second MapReduce job (TriangleJob) that detects triangles.
     */
    private int runTriangleJob(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        final Configuration conf = getConf();
        final Job triangleJob = Job.getInstance(conf, "TriangleJob");
        triangleJob.setJarByClass(RSJoinTriangleCount.class);

        // Add multiple input paths for 2-hop paths and edges, set Mappers
        MultipleInputs.addInputPath(triangleJob, new Path(input), TextInputFormat.class, EdgeMapper.class);
        MultipleInputs.addInputPath(triangleJob, new Path(output + "/Temp"), TextInputFormat.class, TriangleMapper.class);

        triangleJob.setReducerClass(TriangleReducer.class);
        triangleJob.setMapOutputKeyClass(Text.class);
        triangleJob.setMapOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(triangleJob, new Path(output + "/Final")); // Final output for triangle count
        triangleJob.waitForCompletion(true);

        // Fetch and print the global counter for triangle count
        Counters counters = triangleJob.getCounters();
        Counter triangleCounter = counters.findCounter(TriangleMetrics.TRIANGLE_COUNT);
        System.out.println("RS Join Triangles Count: " + triangleCounter.getValue());

        return 1;
    }

    /**
     * The run method orchestrates the execution of both MapReduce jobs.
     */
    @Override
    public int run(final String[] args) throws Exception {
        // Run the PathJob first, followed by the TriangleJob if successful
        if (this.runPathJob(args[0], args[1]) == 0) {
            this.runTriangleJob(args[0], args[1]);
        }
        return 0;
    }

    /**
     * The main method serves as the entry point for the MapReduce job.
     * It invokes the run method via ToolRunner and passes the command-line arguments.
     */
    public static void main(final String[] args) {
        // Ensure that two arguments (input and output paths) are provided
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }

        // Run the MapReduce job using the ToolRunner
        try {
            ToolRunner.run(new RSJoinTriangleCount(), args);
        } catch (final Exception e) {
            log.error("", e); // Log any exceptions that occur
        }
    }
}