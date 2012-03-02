/**
 * 
 */

package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.ac.cam.eng.extraction.hadoop.datatypes.RuleWritable;

/**
 * @author juan MapReduce job to sort source pattern instances according to
 *         HFile order
 */
public class SourcePatternInstanceSortJob extends Configured implements Tool {

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {
        String configFile = args[0];
        Properties p = new Properties();
        p.load(new FileInputStream(configFile));
        Configuration conf = getConf();
        for (String prop: p.stringPropertyNames()) {
            conf.set(prop, p.getProperty(prop));
        }
        Job job = Job.getInstance(new Cluster(conf), conf);
        job.setJarByClass(SourcePatternInstanceSortJob.class);
        job.setJobName("Source pattern instance sorting");
        job.setMapOutputKeyClass(RuleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(RuleWritable.class);
        job.setMapperClass(SwitchKeyValueMapper.class);
        job.setReducerClass(SwitchKeyValueReducer.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setSortComparatorClass(Bytes.ByteArrayComparator.class);
        FileInputFormat.setInputPaths(job, conf.get("inputPaths"));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));
        FileOutputFormat.setCompressOutput(job, true);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage args: configFile");
            System.exit(1);
        }
        int res = ToolRunner.run(new SourcePatternInstanceSortJob(), args);
        System.exit(res);
    }
}
