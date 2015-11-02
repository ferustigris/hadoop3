import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPMapper extends MapReduceBase implements
        org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> out, Reporter reporter) throws IOException {
        String line = value.toString();
        line = line.replaceAll("\\[|\\]", "\"");
        ArrayList<String> fields = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find()) {
            fields.add(m.group(1));
        }

        if (fields.size() < 7) {
            System.out.println("Bad data!");
            System.out.println("fields = " + fields.size());
            for(String s: fields) {
                System.out.println(s);
            }
            return;
        }

        out.collect(new Text(fields.get(0)), new IntWritable(Integer.valueOf(fields.get(6))));
    }
}