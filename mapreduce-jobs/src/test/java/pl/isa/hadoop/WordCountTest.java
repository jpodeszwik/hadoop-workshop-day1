package pl.isa.hadoop;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;

public class WordCountTest {

        @Test
        public void testMapper() throws IOException {
                WordCount.WCMapper mapper = new WordCount.WCMapper();
                MapDriver.newMapDriver(mapper)
                        .withInput(new LongWritable(0), new Text("asdf qwerty asdf"))
                        .withOutput(new Text("asdf"), new LongWritable(1))
                        .withOutput(new Text("qwerty"), new LongWritable(1))
                        .withOutput(new Text("asdf"), new LongWritable(1))
                        .runTest();
        }

        @Test
        public void testReducer() throws IOException {
                WordCount.WCReducer reducer = new WordCount.WCReducer();
                ReduceDriver.newReduceDriver(reducer)
                        .withInput(new Text("asdf"), Lists.newArrayList(new LongWritable(1), new LongWritable(1)))
                        .withInput(new Text("qwerty"), Lists.newArrayList(new LongWritable(1)))
                        .withOutput(new Text("asdf"), new LongWritable(2))
                        .withOutput(new Text("qwerty"), new LongWritable(1))
                        .runTest();
        }

        @Test
        public void testMapreduce() throws IOException {
                WordCount.WCMapper mapper = new WordCount.WCMapper();
                WordCount.WCReducer reducer = new WordCount.WCReducer();

                MapReduceDriver.newMapReduceDriver(mapper, reducer)
                        .withInput(new LongWritable(0), new Text("asdf qwerty asdf"))
                        .withOutput(new Text("asdf"), new LongWritable(2))
                        .withOutput(new Text("qwerty"), new LongWritable(1))
                        .runTest();
        }

}
