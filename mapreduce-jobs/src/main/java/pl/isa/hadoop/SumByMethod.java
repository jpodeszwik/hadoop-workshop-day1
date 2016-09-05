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

public class SumByMethod {
        //94.152.140.254|-|-|2016-09-05 14:21:54|GET /search/tag/list HTTP/1.0|200|5063|http://wilkinson-brown.info/main/category/list/privacy/|Mozilla/5.0 (X11; Linux i686; rv:1.9.6.20) Gecko/2011-07-07 04:32:46 Firefox/3.6.9
        public static class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        String[] parts = value.toString().split("\\|");
                        String methodPart = parts[4];
                        String contentLength = parts[6];
                        String method = methodPart.split("\\s+")[0];

                        context.write(new Text(method), new LongWritable(Long.valueOf(contentLength)));
                }
        }

        public static class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
                protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                        long sum = 0;
                        for(LongWritable val : values) {
                                sum += val.get();
                        }
                        context.write(key, new LongWritable(sum));
                }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
                String input = args[0];
                String output = args[1];

                Job job = Job.getInstance();
                job.setJarByClass(SumByMethod.class);

                job.setMapperClass(WCMapper.class);
                job.setReducerClass(WCReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);

                FileInputFormat.addInputPath(job, new Path(input));
                FileOutputFormat.setOutputPath(job, new Path(output));

                job.waitForCompletion(true);
        }
}
