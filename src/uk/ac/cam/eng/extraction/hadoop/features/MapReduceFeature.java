/**
 * 
 */
package uk.ac.cam.eng.extraction.hadoop.features;

import org.apache.hadoop.conf.Configuration;

import uk.ac.cam.eng.extraction.hadoop.extraction.HadoopJob;

/**
 * @author jmp84
 * 
 */
public interface MapReduceFeature extends HadoopJob {

    public int getNumberOfFeatures(Configuration conf);

}
