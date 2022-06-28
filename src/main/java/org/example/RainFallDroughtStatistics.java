import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * A class represent a map reduce process
 */
public class RainFallDroughtStatistics {

    /**
     * A mapper class
     */
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, Text> {

        private String year;

        /**
         * A map function in order to map (key, value) pairs according to the process
         * @param key - the key-in value
         * @param value - the value-in value
         * @param context - the (key-out, value-out) pairs
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            //the sum of rain fall amount in a specific year
            int sum = 0;

            //extract the year from the line
            if (tokenizer.hasMoreTokens())
                year = tokenizer.nextToken();

            for (int i = 0; i < 12; i++) {
                //extract the rain fall value from the line
                if (tokenizer.hasMoreTokens())
                    sum += Integer.parseInt(tokenizer.nextToken());//add the value to sum
            }

            //extract the average of the year
            //make a pair of (yearAverage, year-avg-sum)
            if (tokenizer.hasMoreTokens())
                context.write(new Text("yearAverage"), new Text(year + "-" + tokenizer.nextToken() + "-" + sum));
        }
    }

    /**
     * A reducer class
     */
    public static class MyReducer extends
            Reducer<Text, Text, Text, IntWritable> {

        Map<String, Integer> yearsAverage = new TreeMap<>();
        Map<String, Integer> yearsAmount = new HashMap<>();
        private int multiAnnualAvg;

        /**
         *  A reduce function in order to aggregate the (key, value) pairs
         * @param key - the key-in value
         * @param values - the values-out values
         * @param context - the (key-out, value-out) pairs
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            //sum and count in  order al calc multi annual average
            int sum = 0;
            int count = 0;
            //aggregate values
            for (Text val : values) {
                //extract info from each value by parsing using split
                String[] tokens = val.toString().split("-");
                String year = tokens[0];
                int avg = Integer.parseInt(tokens[1]);
                int amount = Integer.parseInt(tokens[2]);
                //save info sorted in tree map, in order to detect droughts later
                yearsAverage.put(year, avg);
                yearsAmount.put(year, amount);
                sum += avg;
                count++; //increment years counter
            }
            //calc multi annual average
            multiAnnualAvg = sum / count;

            count = 0;
            //sorted set in order to hold the drought years
            Set<String> years = new TreeSet<>();
            //write the multi annual average
            context.write(new Text("The multi annual average is " + multiAnnualAvg), null);
            //detect drought years
            for (Map.Entry<String, Integer> entry : yearsAverage.entrySet()) {
                String year = entry.getKey();
                //If the annual average is lower than the multi annual average
                //add this year to the set, increment the counter
                if (entry.getValue() < multiAnnualAvg) {
                    years.add(year);
                    count++;
                } else {
                    //If the annual average is not lower than the multi annual average
                    //only if the count >= 3, we detect those years as drought
                    if (count >= 3) {
                        StringBuilder stringBuilder = new StringBuilder("[");
                        int index = 0;
                        for (String y : years)
                        {
                            stringBuilder.append(y).append(" - ").append(yearsAverage.get(y));
                            index++;
                            if(index != years.size())
                                stringBuilder.append(", ");
                        }
                        stringBuilder.append("]");
                        //write the result
                        context.write(new Text("The following years detected as drought " + stringBuilder.toString()), null);
                        stringBuilder.setLength(0);
                    }
                    //clear
                    years.clear();
                    count = 0;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Rainfall drought statistics");
        job.setJarByClass(RainFallDroughtStatistics.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}