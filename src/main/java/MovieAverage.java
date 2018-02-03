/**
 * This code was modified from an in-class example
 * (c) Someone at sometime
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MovieAverage {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        // instance vars
        // ==============
        private Text movieID = new Text();
        private IntWritable rating = new IntWritable();

        // instance methods
        // ==============
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] row = value.toString().split(" ");
                movieID.set(row[0]);
                rating.set(Integer.parseInt(row[1]));
                context.write(movieID, rating);
            } catch(NumberFormatException e) {
                // oh no! parseInt failed!
                // we'll just ignore this for now...
                System.out.println("Parse int failed...");
            }
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        // instance
        // =============
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long count = 0;

            for (IntWritable val : values) {
                sum += val.get();
                count += 1;
            }

            result.set(sum/(double)count);
            context.write(key, result);
        }
    }

    // main
    // =============
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MovieAverage <in> <out>");
            System.exit(2);
        }

        // setup job classes / configuration
        Job job = Job.getInstance(conf, "movieavg");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set job output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set I/O
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
