/**
 * 
 */
package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable3;

/**
 * @author juan
 *
 */
public class Target2Source2Source2TargetJob extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "6");
        Job job = Job.getInstance(new Cluster(conf));
        job.setJarByClass(ExtractorJob.class);
        job.setJobName("Source-to-target to target-to-source conversion");

        // needs to specify the map output key (respectively value) class
        // because
        // it is different than the final output key (respectively value) class
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(PairWritable3.class);
        
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ArrayWritable.class);

        job.setMapperClass(Target2Source2Source2TargetMapper.class);
        //TODO combiner
        job.setReducerClass(Target2Source2Source2TargetReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));
        FileOutputFormat.setCompressOutput(job, true);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
        }
        int res = ToolRunner.run(new Configuration(), new ExtractorJob(), args);
        System.exit(res);
    }
}
