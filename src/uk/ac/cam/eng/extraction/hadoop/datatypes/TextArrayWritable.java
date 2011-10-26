/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

/**
 * @author jmp84 This class extends ArrayWritable because it is input to the
 *         reducer in the map-reduce framework (see hadoop doc on ArrayWritable)
 */
public class TextArrayWritable extends ArrayWritable {

    public TextArrayWritable() {
        super(Text.class);
    }
}
