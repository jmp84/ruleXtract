/**
 * 
 */
package uk.ac.cam.eng.rulebuilding.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author juan
 *
 */
public class SwitchKeyValueReducer<K, V> extends Reducer<K, V, V, K>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(K key, Iterable<V> listValues, Context context)
			throws IOException, InterruptedException {
		for (V value: listValues) {
			context.write(value, key);
		}
	}
}
