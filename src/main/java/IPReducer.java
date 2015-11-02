import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class IPReducer extends MapReduceBase implements
        org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, Pair> {

    Pair p = new Pair();

    public void reduce(Text ip, Iterator<IntWritable> values, OutputCollector<Text, Pair> out, Reporter reporter) throws IOException {
        int sum = 0;
        int count = 0;
        while (values.hasNext()) {
            sum += values.next().get();
            count++;
        }

        //Dumping the output
        p.set(sum, sum * 1.0 / count);
        out.collect(ip, p);
    }
}
