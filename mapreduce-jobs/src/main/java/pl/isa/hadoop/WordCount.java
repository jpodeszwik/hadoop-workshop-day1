package pl.isa.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
        public static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

                @Override
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        String text = value.toString();
                        for(String word : text.split("\\s+")) {
                                context.write(new Text(word), new LongWritable(1));
                        }

                }
        }

        public static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
                @Override
                protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                        long sum = 0;
                        for(LongWritable value : values) {
                                sum += value.get();
                        }
                        context.write(key, new LongWritable(sum));
                }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
                String input = args[0];
                String output = args[1];

                Job job = Job.getInstance();
                job.setJarByClass(WordCount.class);
                job.setMapperClass(WCMapper.class);
                job.setReducerClass(WCReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                FileInputFormat.addInputPath(job, new Path(input));
                FileOutputFormat.setOutputPath(job, new Path(output));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
