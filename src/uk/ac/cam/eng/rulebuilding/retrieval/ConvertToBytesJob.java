/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.retrieval;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable2;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author jmp84 This class runs a mapreduce job to convert a SequenceFile of
 *         RuleWritable and DoubleWritable to a SequenceFile of BytesWritable
 *         and BytesWritable
 */
public class ConvertToBytesJob extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "6");
        Job job = Job.getInstance(new Cluster(conf));
        job.setJarByClass(ConvertToBytesJob.class);
        job.setJobName("Convert to Bytes");

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(PairWritable2.class);

        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ArrayWritable.class);

        job.setMapperClass(ConvertToBytesMapper.class);
        job.setReducerClass(ConvertToBytesReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setCompressOutput(job, true);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage args: inputFile outputFile");
        }
        int res = ToolRunner.run(new Configuration(), new ConvertToBytesJob(),
                args);
        System.exit(res);
    }
}
