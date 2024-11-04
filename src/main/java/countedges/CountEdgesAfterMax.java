package countedges;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This MapReduce program counts the number of edges remaining in the dataset
 * after applying the MAX filter, which filters edges based on the user IDs.
 */
public class CountEdgesAfterMax extends Configured implements Tool {

    private static final Logger log = LogManager.getLogger(CountEdgesAfterMax.class);

    // Global Counter to keep track of the number of edges after applying the MAX filter
    public enum EdgeCounter {
        NUM_EDGES
    }

    // Constant to define the MAX value, i.e., the threshold for filtering user IDs
    public static int MAX_LIMIT = 11316812;  // Change this value as needed

    /**
     * Mapper class that filters edges and counts the remaining edges.
     */
    public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {

        // Mapper method to filter and count edges
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Split the input line (edge) into source node (X) and target node (Y)
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            while (tokenizer.hasMoreTokens()) {
                String[] userIDs = tokenizer.nextToken().split(",");
                String source = userIDs[0];  // X
                String target = userIDs[1];  // Y

                // Convert to integers for comparison
                int sourceID = Integer.parseInt(source);
                int targetID = Integer.parseInt(target);

                // Only consider edges where both source and target IDs are less than MAX
                if (sourceID < MAX_LIMIT && targetID < MAX_LIMIT) {
                    // Increment the global counter for remaining edges
                    context.getCounter(EdgeCounter.NUM_EDGES).increment(1);
                }
            }
        }
    }

    /**
     * Main method to configure and run the MapReduce job.
     */
    @Override
    public int run(final String[] args) throws Exception {
        // Set up the configuration and the job
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Count Edges After MAX Filter");
        job.setJarByClass(CountEdgesAfterMax.class);
        job.setMapperClass(EdgeMapper.class);

        // Set output types for the mapper
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input and output paths are provided via command-line arguments (like in the Approx code)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job
        boolean success = job.waitForCompletion(true);

        // Retrieve the value of the global counter after the job completes
        Counter counter = job.getCounters().findCounter(EdgeCounter.NUM_EDGES);
        System.out.println("Number of edges after applying MAX filter: " + counter.getValue());

        return success ? 0 : 1;
    }

    /**
     * Main entry point for the program.
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required: <input-path> <output-path>");
        }

        try {
            ToolRunner.run(new CountEdgesAfterMax(), args);
        } catch (final Exception e) {
            log.error("Error running the MapReduce job", e);
        }
    }
}