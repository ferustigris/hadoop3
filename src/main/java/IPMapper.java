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
    Text ip = new Text();
    IntWritable count = new IntWritable();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> out, Reporter reporter) throws IOException {
        String line = value.toString();
        line = line.replaceAll("\\[|\\]", "\"");
        ArrayList<String> fields = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find()) {
            fields.add(m.group(1));
        }

        if (fields.size() < 9) {
            System.out.println("Bad data!");
            System.out.println("fields = " + fields.size());
            for(String s: fields) {
                System.out.println(s);
            }
            return;
        }

        String browserParams[] = fields.get(8).split("\\s");
        if (browserParams.length > 0) {
            String browser = browserParams[0];
            reporter.getCounter("browsers", browser).increment(1    );
        }

        ip.set(fields.get(0));
        count.set(Integer.valueOf(fields.get(6)));
        out.collect(ip, count);
    }
}