package edu.ucr.cs.cs236;
import java.util.Collections;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.PriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Task2 {

    // Class to maintain key value pair and get 3 top states with positive change
    public static class KeyValuePair implements WritableComparable<KeyValuePair> {
        private Text key;
        private DoubleWritable value;

        public KeyValuePair() {
            this.key = new Text();
            this.value = new DoubleWritable();
        }

        public KeyValuePair(Text key, DoubleWritable value) {
            this.key = new Text(key);
            this.value = new DoubleWritable(value.get());
        }

        @Override
        public void write(DataOutput out) throws IOException {
            key.write(out);
            value.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            key.readFields(in);
            value.readFields(in);
        }

        @Override
        public int compareTo(KeyValuePair other) {
            return value.compareTo(other.value);
        }

        public Text getKey() {
            return key;
        }

        public DoubleWritable getValue() {
            return value;
        }
    }


    // Mapper class to get the store values of each County.
    // Input is row of dataset.
    // Output - key = State:VariableCode and value = float(Store value)
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text groupKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            
            String currentLine = value.toString();
            String[] tokens = currentLine.split(",");

             if (tokens.length < 5) {
            System.out.println("Skipping malformed line: " + currentLine);
            return;
        }

        String state = tokens[1].trim();
        String variableCode = tokens[3].trim();
        String valueStr = tokens[4].trim();

        // Process only relevant variable codes
        if (!variableCode.equals("GROCPTH11") && !variableCode.equals("GROCPTH16")) {
            return;
        }
        try {
            double parsedValue = Double.parseDouble(valueStr);
            groupKey.set(state + ":" + variableCode);
            outputValue.set(parsedValue);
            context.write(groupKey, outputValue);
        } catch (NumberFormatException e) {
            System.out.println("Skipping invalid value: " + valueStr + " in line: " + currentLine);
        }
    }
}
    // Reducer to calculate avg stores for each state
    // Input - key = State:VariableCode and value = float(Store value)
    // Output - key = State:VariableCode and value = avg stores for that State
    public static class IntSumReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                          Context context
        ) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum/count);
            context.write(key, result);
        }
    }


    // Mapper for calculating the difference between 2016 - 2011 avgStores
    // Input - row of written file by job1
    // Output - key = State:VariableCode and value = avg stores for that State
    public static class TokenizerMapper2
            extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text groupKey = new Text();
        private DoubleWritable outputValue = new DoubleWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String currentLine = value.toString();
            String[] tokens = currentLine.split(",");
            if (tokens.length < 2) {
                System.out.println("Skipping malformed line: " + currentLine);
                return;
            }
            try {
                String group = tokens[0].trim(); // Key (e.g., "State:VariableCode")
                double colValue = Double.parseDouble(tokens[1].trim()); // Value (avg stores)
    
                groupKey.set(group); // Set the key as State:VariableCode
                outputValue.set(colValue); // Set the value as avg stores
                context.write(groupKey, outputValue);
    
            } catch (NumberFormatException e) {
                System.out.println("Skipping invalid value in line: " + currentLine);
            }
        }
    }


    // Combiner for combining as per key: State
    // Input - key = State:VariableCode and value = avg stores for that State
    // Output - key = State and value = avg stores for that State
    public static class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (DoubleWritable val : values) {
                Text key2 = new Text();
                key2.set(key.toString().split(":")[0].trim());
                
                // System.out.println("Key "+ key2.toString() +" Value "+ Double.toString(val.get()));
                context.write(key2, new DoubleWritable(val.get()));
            }
            
        }
    }


    // Reducer to calculate difference between 2016 and 2011 avgStores
    // Input - key = State and value = avg stores for that State
    // Output - key = State and value = diff of avgStores for that State
    public static class IntSumReducer2
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
        private Text key2 = new Text();
        private PriorityQueue<KeyValuePair> topValues;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            topValues = new PriorityQueue <KeyValuePair>(3);
        }

        public void reduce(Text key, Iterable<DoubleWritable> values,
                          Context context
        ) throws IOException, InterruptedException {

            double sum = 0;
            int i = 0;
            for (DoubleWritable val : values) {
                
                // Combiner writes first value of 2011 for each state always
                if(i == 0){
                    sum -= val.get();
                } else{
                    sum += val.get();
                }
                i++;
                
            }
            
            result.set(sum);

            topValues.add(new KeyValuePair(key, result));
            if (topValues.size() > 3) {
                topValues.poll();
            }
            
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top 3 values for this reducer
            while (!topValues.isEmpty()) {
                KeyValuePair pair = topValues.poll();
                context.write(pair.getKey(), pair.getValue());
            }
        }
    }


    // Mapper for reading output of job1
    // Input - row of written file by job1
    // Output - key = State and value = VariableCode:AvgStores
    public static class File1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length == 2) {
                String[] stateValue = parts[0].split(":");
                if (stateValue.length == 2) {
                    String state = stateValue[0].trim();
                    String variableCode = stateValue[1].trim();
                    String val = parts[1].trim();
                    context.write(new Text(state), new Text(variableCode + ":" + val));
                }
            }
        }
    }


    // Mapper for reading output of job2
    // Input - row of written file by job2
    // Output - key = State and value = DIFF:differenceValue
    public static class File2Mapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length == 2) {
                String state = parts[0].trim();
                String diff = parts[1].trim();
                context.write(new Text(state), new Text("DIFF:" + diff));
            }
        }
    }  


    // Reducer for joining the outputs of job1 and job2
    // Input -  key = State and value = DIFF:differenceValue or VariableCode:AvgStores
    // Output - key = State and value = AvgStores2011 \t AvgStores2016 \t Difference
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String grocPTH11 = null;
            String grocPTH16 = null;
            String diff = null;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts[0].equals("GROCPTH11")) {
                    grocPTH11 = parts[1];
                } else if (parts[0].equals("GROCPTH16")) {
                    grocPTH16 = parts[1];
                } else if (parts[0].equals("DIFF")) {
                    diff = parts[1];
                }
            }

            if (grocPTH11 != null && grocPTH16 != null && diff != null) {
                context.write(key, new Text(grocPTH11 + "\t" + grocPTH16 + "\t" + diff));
            }
        }
    }


    // Main function
    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();
        Configuration conf = new Configuration();

        // Job1 for getting the avgStores for 2011 and 2016 year of each State
        Job job = Job.getInstance(conf, "get_state_by_store_avg");
        job.setJarByClass(Task2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Job2 for getting the difference between avgStores of 2011 and 2016 year for each State
        Job job2 = Job.getInstance(conf, "get_state_by_store_diff");

        if(job.waitForCompletion(true)){       
            job2.setJarByClass(Task2.class);
            job2.setMapperClass(TokenizerMapper2.class);
            job2.setCombinerClass(SumCombiner.class);
            job2.setReducerClass(IntSumReducer2.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        }

        // Job3 for joining the output of job1 and job2
        Job job3 = Job.getInstance(conf, "state_data_join");

        if(job2.waitForCompletion(true)){
            job3.setJarByClass(Task2.class);
            job3.setReducerClass(JoinReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class, File1Mapper.class);
            MultipleInputs.addInputPath(job3, new Path(args[2]), TextInputFormat.class, File2Mapper.class);
            FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        }

        boolean success = job3.waitForCompletion(true);
        long endTime = System.nanoTime();
        System.out.println("Time required: " + endTime);
        double timeRequired = (endTime - startTime) / 1e9;
        System.out.printf("Time required: %.4f seconds\n", timeRequired);
        System.exit(success ? 0 : 1);
    }
}