/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author juan
 *
 */
public class SwitchKeyValueMapper<K, V> extends Mapper<K, V, V, K> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(K key, V value, Context context)
			throws IOException, InterruptedException {
		context.write(value, key);
	}
}
