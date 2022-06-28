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
public class RainFallSeasonsStatistics {

    /**
     * A mapper class
     */
    public static class MyMapper extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private IntWritable rainfallValue = new IntWritable();
        private Text year = new Text();

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

                //make a pair of (year-season, rainfallValue)
                if (i == 0 || i == 1 || i == 11) {//winter
                    context.write(new Text(year + "-winter"), rainfallValue);
                } else if (i == 2 || i == 3 || i == 4) {//spring
                    context.write(new Text(year + "-spring"), rainfallValue);
                } else if (i == 5 || i == 6 || i == 7) {//summer
                    context.write(new Text(year + "-summer"), rainfallValue);
                } else//fall
                {
                    context.write(new Text(year + "-fall"), rainfallValue);
                }
            }
        }
    }

    /**
     * A reducer class
     */
    public static class MyReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private int max = Integer.MIN_VALUE;
        private int min = Integer.MAX_VALUE;
        private IntWritable sum = new IntWritable();
        private String maxSeasonYear;
        private String minSeasonYear;

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
            this.sum.set(sum);
            //make a pair of (year-season, totalRainfall)
            context.write(key, this.sum);

            //find the year-season with the max rain fall value
            if (sum > max) {
                max = sum;
                maxSeasonYear = key.toString();
            }

            //find the year-season with the min rain fall value
            if (sum < min) {
                min = sum;
                minSeasonYear = key.toString();
            }
        }

        /**
         * A cleanup function executes after the reduce process
         * @param context - the (key-out, value-out) pairs
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("The season and year in which the largest precipitation fell are " + maxSeasonYear +
                    ", the amount is "), new IntWritable(max));
            context.write(new Text("The season and year in which the smallest precipitation fell are " + minSeasonYear +
                    ", the amount is "), new IntWritable(min));
        }
    }


    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "Rainfall seasons statistics");
        job.setJarByClass(RainFallSeasonsStatistics.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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