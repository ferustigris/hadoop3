import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class IPReducer extends MapReduceBase implements
        org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text ip, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> out, Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }

        //Dumping the output
        out.collect(ip, new IntWritable(sum));
    }
}
