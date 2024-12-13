package edu.ucr.cs.cs236;


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {

    // Mapper class to process the input data and emit key-value pairs
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        
        private Text groupKey = new Text(); // Key for the state
        private IntWritable outputValue = new IntWritable(); // Value for the population

        // Map function processes each line of the input
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String currentLine = value.toString(); // Convert input line to string

            // Split the line by commas into tokens
            String[] tokens = currentLine.split(",");

            // Extract the state name (column 1) and trim any whitespace
            String group = tokens[1].trim(); 

            // Variable to filter rows based on the specific variable code
            String variable_to_sum = "2010_Census_Population";

            // Check if the variable code matches the desired one
            if (tokens[3].equals(variable_to_sum)) {
                try {
                    // Parse the population value (column 4) as an integer
                    int colValue = Integer.parseInt(tokens[4]);

                    // Set the key as the state name
                    groupKey.set(group);

                    // Set the value as the county population
                    outputValue.set(colValue);

                    // Write the key-value pair to the context
                    context.write(groupKey, outputValue);
                } catch (NumberFormatException e) {
                    // Handle invalid population values (e.g., non-numeric data)
                }
            }
        }
    }

    // Reducer class to aggregate populations for each state
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable(); // Final aggregated value

        // Reduce function aggregates values for each key (state)
        public void reduce(Text key, Iterable<IntWritable> values,
                          Context context
        ) throws IOException, InterruptedException {

            int sum = 0; // Initialize sum for the state's population
            for (IntWritable val : values) {
                // Add up all population values for the state
                sum += val.get();
            }
            result.set(sum); // Set the aggregated result
            context.write(key, result); // Write the state and its total population
        }
    }

    // Driver code to configure and run the MapReduce job
    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime(); // Record the start time
        System.out.println("Start time: " + startTime);

        Configuration conf = new Configuration(); // Create a Hadoop configuration
        Job job = Job.getInstance(conf, "get_state_by_pop_2010"); // Initialize the job with a name
        job.setJarByClass(App.class); // Set the main class containing the job
        job.setMapperClass(TokenizerMapper.class); // Set the mapper class
        job.setCombinerClass(IntSumReducer.class); // Optional combiner (same as reducer here)
        job.setReducerClass(IntSumReducer.class); // Set the reducer class
        job.setOutputKeyClass(Text.class); // Set output key type (Text for state name)
        job.setOutputValueClass(IntWritable.class); // Set output value type (IntWritable for population)

        // Set input file path (args[0]) and output file path (args[1])
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete and capture its success status
        boolean success = job.waitForCompletion(true);

        long endTime = System.nanoTime(); // Record the end time
        System.out.println("Time required: " + endTime);
        double timeRequired = (endTime - startTime) / 1e9; // Calculate total execution time in seconds
        System.out.printf("Time required: %.4f seconds\n", timeRequired);

        System.exit(success ? 0 : 1); // Exit based on job success or failure
    }
}
