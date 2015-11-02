import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class IPCounter {
    public static void main(String[] args) throws IOException {

        if(args.length < 2) {
            throw new IllegalArgumentException("Not enough args");
        }

        //Creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(IPCounter.class);
        conf.setJobName("IPCount");

        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //Setting configuration object with the Data Type of output Key and Value of mapper
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        //Providing the mapper and reducer class names
        conf.setMapperClass(IPMapper.class);
        conf.setReducerClass(IPReducer.class);

        //Setting format of input and output
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        //The hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //Running the job
        RunningJob j = JobClient.runJob(conf);

        Iterator<Counters.Counter> i = j.getCounters().getGroup("browsers").iterator();

        while (i.hasNext()) {
            Counters.Counter counter = i.next();
            System.out.println(counter.getDisplayName() + ", " + counter.getCounter());
        }

    }
}
