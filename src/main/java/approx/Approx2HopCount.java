package approx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This class implements an approximate 2-hop path counting algorithm in MapReduce.
 * The goal is to compute the number of 2-hop paths, i.e., where a user X follows user Y,
 * and user Y follows user Z, but only considering users with IDs less than a given MAX_LIMIT.
 */
public class Approx2HopCount extends Configured implements Tool {

    // Enum for maintaining global counters across the job
    enum GlobalMetrics {
        HOP_COUNT_APPROX // Counter to keep track of the total number of approximate 2-hop paths
    }

    // Logger for logging messages
    private static final Logger log = LogManager.getLogger(Approx2HopCount.class);
    public static IntWritable approxHopSum = new IntWritable(0);

    // Constant to define the MAX_LIMIT, i.e., the threshold for filtering user IDs
    public static int MAX_LIMIT = 12500;

    /**
     * Mapper class that processes each line of input, splits the user IDs, and emits key-value pairs
     * where the key is a user ID, and the value is a formatted string with direction (IN/OUT).
     */
    public static class ApproxMapper extends Mapper<Object, Text, Text, Text> {

        // Variable to store the user ID
        private final Text userID = new Text();

        /**
         * The map method processes each input line and emits two key-value pairs:
         * - (X, "OUT") indicating that X follows Y (outgoing edge from X)
         * - (Y, "IN") indicating that Y is followed by X (incoming edge to Y)
         * This only occurs if both user IDs are less than the MAX_LIMIT.
         */
        @Override
        public void map(final Object inputKey, final Text inputValue, final Context context) throws IOException, InterruptedException {
            // Tokenize the input line (assumed to be a CSV format)
            final StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());

            // Iterate over the tokenized input (assuming each line contains a pair of user IDs)
            while (tokenizer.hasMoreTokens()) {
                String[] userIDs = tokenizer.nextToken().split(",");

                // Only process user IDs that are below the MAX_LIMIT
                if ((Integer.parseInt(userIDs[0]) < MAX_LIMIT) && (Integer.parseInt(userIDs[1]) < MAX_LIMIT)) {
                    // First user (follower) has an outgoing edge
                    userID.set(userIDs[0]);
                    context.write(userID, new Text(userIDs[0] + "," + userIDs[1] + "," + "OUT"));

                    // Second user (followed) has an incoming edge
                    userID.set(userIDs[1]);
                    context.write(userID, new Text(userIDs[0] + "," + userIDs[1] + "," + "IN"));
                }
            }
        }
    }

    /**
     * Reducer class that processes the values for each user and counts the number of 2-hop paths.
     * It separates the incoming and outgoing edges and calculates the number of 2-hop paths
     * by multiplying the number of incoming edges by the number of outgoing edges.
     */
    public static class ApproxReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * The reduce method processes the list of values for each user (key), separates them into
         * incoming and outgoing edges, and computes the number of 2-hop paths.
         * It then emits a record for each 2-hop path found.
         */
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

            // Lists to store incoming and outgoing edges
            List<String> incomingList = new ArrayList<>();
            List<String> outgoingList = new ArrayList<>();

            // Iterate over the values and separate incoming and outgoing edges
            for (final Text val : values) {
                String[] tokens = val.toString().split(",");
                if (tokens[2].equals("IN")) {
                    incomingList.add(tokens[0]); // Add to the list of incoming edges
                } else {
                    outgoingList.add(tokens[1]); // Add to the list of outgoing edges
                }
            }

            // Generate 2-hop paths by combining incoming and outgoing edges
            for (int i = 0; i < incomingList.size(); i++) {
                for (int j = 0; j < outgoingList.size(); j++) {
                    // Emit the 2-hop path (X, Y, Z)
                    context.write(new Text("2HopEdge"), new Text(incomingList.get(i) + "," + key.toString() + "," + outgoingList.get(j)));
                }
            }

            // Increment the global counter by the number of 2-hop paths found for this user
            context.getCounter(GlobalMetrics.HOP_COUNT_APPROX).increment(incomingList.size() * outgoingList.size());
        }
    }

    /**
     * The run method sets up and configures the MapReduce job.
     * It specifies the input and output paths, mapper and reducer classes, and handles job execution.
     */
    @Override
    public int run(final String[] args) throws Exception {
        // Create a new MapReduce job with the provided configuration
        final Configuration configuration = getConf();
        final Job job = Job.getInstance(configuration, "Approx 2-Hop Count");

        // Set the Jar class containing the main method (this class)
        job.setJarByClass(Approx2HopCount.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(ApproxMapper.class);
        job.setReducerClass(ApproxReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Specify the input and output paths for the job
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input directory
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory

        // Wait for the job to complete and retrieve its completion status
        int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;

        // Fetch the global counter for 2-hop paths and print the final count
        Counter counter = job.getCounters().findCounter(GlobalMetrics.HOP_COUNT_APPROX);
        System.out.println("Approx 2 Hop Paths Count: " + counter.getValue());

        return jobCompletionStatus; // Return the job status (0 for success)
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
            ToolRunner.run(new Approx2HopCount(), args);
        } catch (final Exception e) {
            log.error("", e); // Log any exceptions that occur
        }
    }
}