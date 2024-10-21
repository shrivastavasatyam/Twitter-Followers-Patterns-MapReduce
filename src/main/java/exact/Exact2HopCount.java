package exact;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
 * This class implements an exact 2-hop path counting algorithm in MapReduce.
 * The goal is to compute the number of 2-hop paths, i.e., where a user X follows user Y,
 * and user Y follows user Z.
 */
public class Exact2HopCount extends Configured implements Tool {

    // Enum for maintaining global counters across the job
    enum GlobalMetrics {
        HOP_COUNT // Counter to keep track of total number of 2-hop paths
    }

    // Logger for logging messages
    private static final Logger log = LogManager.getLogger(Exact2HopCount.class);
    IntWritable hopSum = new IntWritable(0);

    // This class implements the Mapper to process each input line and emit key-value pairs.
    public static class HopMapper extends Mapper<Object, Text, Text, Text> {

        // Variables to store userID and direction (IN or OUT) for each edge
        private final Text userID = new Text();
        private final Text direction = new Text();

        /**
         * The map method processes each line from the input file, which contains two user IDs.
         * For each line (X, Y), it emits two key-value pairs:
         * - (X, "OUT") indicating that X follows Y (outgoing edge from X)
         * - (Y, "IN") indicating that Y is followed by X (incoming edge to Y)
         */
        @Override
        public void map(final Object inputKey, final Text inputValue, final Context context) throws IOException, InterruptedException {
            // Tokenize the input line (assumed to be a CSV format)
            final StringTokenizer tokenizer = new StringTokenizer(inputValue.toString());

            // Iterate over the tokenized input (assuming each line contains a pair of users)
            while (tokenizer.hasMoreTokens()) {
                String[] users = tokenizer.nextToken().split(",");

                // First user (follower) has an outgoing edge
                userID.set(users[0]);
                direction.set("OUT"); // Mark this as an outgoing edge
                context.write(userID, direction);

                // Second user (followed) has an incoming edge
                userID.set(users[1]);
                direction.set("IN"); // Mark this as an incoming edge
                context.write(userID, direction);
            }
        }
    }

    // This class implements the Reducer to count the 2-hop paths for each user.
    public static class HopReducer extends Reducer<Text, Text, Text, LongWritable> {

        /**
         * The reduce method receives a userID as the key and a list of "IN"/"OUT" directions as values.
         * It counts the number of incoming and outgoing edges for this user and computes the number of
         * 2-hop paths passing through this user by multiplying the incoming and outgoing counts.
         */
        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            Text outgoingEdge = new Text("OUT");
            Text incomingEdge = new Text("IN");

            // Variables to store the number of incoming and outgoing edges
            long incomingCount = 0;
            long outgoingCount = 0;

            // Iterate over the list of values (directions)
            for (final Text value : values) {
                // Count the number of incoming and outgoing edges for the user
                if (value.equals(outgoingEdge)) {
                    outgoingCount += 1;
                } else if (value.equals(incomingEdge)) {
                    incomingCount += 1;
                }
            }

            // The number of 2-hop paths through this user is the product of incoming and outgoing edges
            long totalCount = incomingCount * outgoingCount;

            // Increment the global counter for 2-hop paths
            context.getCounter(GlobalMetrics.HOP_COUNT).increment(totalCount);
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
        final Job job = Job.getInstance(configuration, "Exact 2-Hop Count");

        // Set the Jar class containing the main method (this class)
        job.setJarByClass(Exact2HopCount.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(HopMapper.class);
        job.setReducerClass(HopReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Specify the input and output paths for the job
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input directory
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory

        // Wait for the job to complete and retrieve its completion status
        int jobCompletionStatus = job.waitForCompletion(true) ? 0 : 1;

        // Fetch the global counter for 2-hop paths and print the final count
        Counter counter = job.getCounters().findCounter(GlobalMetrics.HOP_COUNT);
        System.out.println("Exact 2 Hop Paths Count: " + counter.getValue());

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
            ToolRunner.run(new Exact2HopCount(), args);
        } catch (final Exception e) {
            log.error("", e); // Log any exceptions that occur
        }
    }
}