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
import java.util.StringTokenizer;

/**
 * A class represent a map reduce process
 */
public class RainFallMinMaxStatistics {

    /**
     * A mapper class
     */
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable rainfallValue = new IntWritable();
        private Text year = new Text();
        private int max = Integer.MIN_VALUE;//min value in order to find the maximum
        private int min = Integer.MAX_VALUE;//max value in order to find the minimum
        private String maxMonth;
        private String minMonth;

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

            //extract the year from the line
            if (tokenizer.hasMoreTokens())
                year.set(tokenizer.nextToken());

            for (int i = 0; i < 12; i++) {
                //extract the rain fall value from the line
                if (tokenizer.hasMoreTokens()) {
                    int rainValue = Integer.parseInt(tokenizer.nextToken());
                    rainfallValue.set(rainValue);
                }

                //make a pair of (year, rainfallValue)
                context.write(year, rainfallValue);

                //find the month/year with the max rain fall value
                if (rainfallValue.get() > max) {
                    max = rainfallValue.get();
                    maxMonth = (i + 1) + "/" + year;
                }

                //find the month/year with the min rain fall value
                if (rainfallValue.get() < min) {
                    min = rainfallValue.get();
                    minMonth = (i + 1) + "/" + year;
                }
            }
        }

        /**
         * A cleanup function executes after the map process
         * @param context - the (key-out, value-out) pairs
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("maxMonth-" + maxMonth), new IntWritable(max));
            context.write(new Text("minMonth-" + minMonth), new IntWritable(min));
        }
    }

    /**
     * A reducer class
     */
    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private int max = Integer.MIN_VALUE;//min value in order to find the maximum
        private int min = Integer.MAX_VALUE;//max value in order to find the minimum
        private IntWritable sum = new IntWritable();
        private String maxYear;
        private String minYear;
        private String maxMonth;
        private String minMonth;
        private int maxMonthVal;
        private int minMonthVal;

        /**
         * A reduce function in order to aggregate the (key, value) pairs
         * @param key - the key-in value
         * @param values - the values-out values
         * @param context - the (key-out, value-out) pairs
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;

            //aggregate values
            for (IntWritable val : values) {
                sum += val.get();
            }

            //extract the month/year max rainfall value from the map process
            if(key.toString().contains("maxMonth"))
            {
                maxMonth = key.toString().split("-")[1];
                maxMonthVal = sum;

            } //extract the month/year min rainfall value from the map process
            else if (key.toString().contains("minMonth"))
            {
                minMonth = key.toString().split("-")[1];
                minMonthVal = sum;

            }
            else {
                this.sum.set(sum);
                //make a pair of (year, totalRainfall)
                context.write(key, this.sum);

                //find the year with the max rain fall value
                if(sum > max)
                {
                    max = sum;
                    maxYear = key.toString();
                }

                //find the year with the min rain fall value
                if(sum < min)
                {
                    min = sum;
                    minYear = key.toString();
                }
            }
        }

        /**
         *  A cleanup function executes after the reduce process
         * @param context - the (key-out, value-out) pairs
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("The month and year in which the greatest amount of precipitation fell are " + maxMonth +
                    ", the amount is "), new IntWritable(maxMonthVal));
            context.write(new Text("The month and year in which the lowest amount of precipitation fell are " + minMonth +
                    ", the amount is "), new IntWritable(minMonthVal));
            context.write(new Text("The year in which the biggest amount of precipitation fell is " + maxYear +
                    ", the amount is "), new IntWritable(max));
            context.write(new Text("The year in which the smallest amount of precipitation fell is " + minYear +
                    ", the amount is "), new IntWritable(min));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Rainfall min max statistics");
        job.setJarByClass(RainFallMinMaxStatistics.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
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