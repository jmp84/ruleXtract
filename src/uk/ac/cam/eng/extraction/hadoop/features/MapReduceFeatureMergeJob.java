/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.features;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.GeneralPairWritable;

/**
 * @author jmp84 MapReduce job that takes the output of all MapReduce features
 *         and converts them to an HFile that will be processed by the retrieval
 *         part
 */
public class MapReduceFeatureMergeJob extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        String configFile = args[0];
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configFile));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Configuration conf = getConf();
        for (String prop : p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        Job job = Job.getInstance(new Cluster(conf), conf);
        job.setJarByClass(MapReduceFeatureMergeJob.class);
        job.setJobName("mapReduceFeaturesMerge");
        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(GeneralPairWritable.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(GeneralPairWritable.class);
        job.setMapperClass(MapReduceFeatureMergeMapper.class);
        job.setReducerClass(MapReduceFeatureMergeReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));
        FileOutputFormat.setCompressOutput(job, true);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
            System.exit(1);
        }
        int res = ToolRunner.run(new Source2TargetProbabilityJob(), args);
        System.exit(res);
    }
}
