/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * @author jmp84 Interface used for Hadoop workflow. Simply returns a Hadoop
 *         job.
 */
public interface HadoopJob {

    public Job getJob(Configuration conf) throws IOException;
}
