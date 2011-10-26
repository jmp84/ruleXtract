
package uk.ac.cam.eng.extraction.hadoop.extraction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.PairWritable;
import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

public class ExtractorJobMethod1 extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // conf.set("mapred.job.tracker", "local");
        // conf.set("fs.default.name", "local");
        // Job job = new Job(conf);
        Job job = Job.getInstance(new Cluster(conf));
        job.setJarByClass(ExtractorJobMethod1.class);
        job.setJobName("Rule Extraction");

        job.setOutputKeyClass(RuleWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(ExtractorMapper1Method1.class);
        job.setReducerClass(ExtractorReducer1Method1.class);
        // job.setCombinerClass(RuleReducer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outpath = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);
        hdfs.delete(outpath, true);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new ExtractorJobMethod1(), args);
        System.exit(res);
    }

}
