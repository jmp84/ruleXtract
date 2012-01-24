/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import uk.ac.cam.eng.rulebuilding.retrieval.RulePattern;

/**
 * @author jmp84 This class represents a writable rule pattern, it's essentially
 *         the same as a RulePattern but is convenient to be used in the
 *         map-reduced framework
 */
public class RulePatternWritable implements
        WritableComparable<RulePatternWritable> {

    private Text source;
    private Text target;

    /**
     * @return the source
     */
    public Text getSource() {
        return source;
    }

    /**
     * @param source
     *            the source to set
     */
    public void setSource(Text source) {
        this.source = source;
    }

    /**
     * @return the target
     */
    public Text getTarget() {
        return target;
    }

    /**
     * @param target
     *            the target to set
     */
    public void setTarget(Text target) {
        this.target = target;
    }

    public RulePatternWritable() {
        source = new Text();
        target = new Text();
    }

    public RulePatternWritable(RulePattern r) {
        String[] parts = r.toString().split("\\s+");
        if (parts.length != 2) {
            System.err.println("Rule malformed: " + r.toString());
            System.exit(1);
        }
        source = new Text(parts[0]);
        target = new Text(parts[1]);
    }

    public RulePatternWritable(RulePatternWritable source,
            RulePatternWritable target) {
        this.source = source.source;
        this.target = target.target;
    }

    public static RulePatternWritable makeSourceMarginal(RulePattern r) {
        String[] parts = r.toString().split("\\s+");
        if (parts.length != 2) {
            System.err.println("Rule malformed: " + r.toString());
            System.exit(1);
        }
        RulePatternWritable res = new RulePatternWritable();
        res.source = new Text(parts[0]);
        res.target = new Text();
        return res;
    }

    public static RulePatternWritable makeSourceMarginal(
            RulePatternWritable r) {
        RulePatternWritable res = new RulePatternWritable();
        res.source = new Text(r.source);
        res.target = new Text();
        return res;
    }

    public static RulePatternWritable makeTargetMarginal(RulePattern r) {
        String[] parts = r.toString().split("\\s+");
        if (parts.length != 2) {
            System.err.println("Rule malformed: " + r.toString());
            System.exit(1);
        }
        RulePatternWritable res = new RulePatternWritable();
        res.target = new Text(parts[1]);
        res.source = new Text();
        return res;
    }

    public static RulePatternWritable makeTargetMarginal(
            RulePatternWritable r) {
        RulePatternWritable res = new RulePatternWritable();
        res.source = new Text();
        res.target = new Text(r.target);
        return res;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(source);
        sb.append(" ");
        sb.append(target);
        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput arg0) throws IOException {
        source.readFields(arg0);
        target.readFields(arg0);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput arg0) throws IOException {
        source.write(arg0);
        target.write(arg0);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(RulePatternWritable arg0) {
        int cmp = source.compareTo(arg0.source);
        if (cmp != 0)
            return cmp;
        // TODO return directly
        cmp = target.compareTo(arg0.target);
        return cmp;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((source == null) ? 0 : source.hashCode());
        result = prime * result + ((target == null) ? 0 : target.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RulePatternWritable other = (RulePatternWritable) obj;
        if (source == null) {
            if (other.source != null)
                return false;
        }
        else if (!source.equals(other.source))
            return false;
        if (target == null) {
            if (other.target != null)
                return false;
        }
        else if (!target.equals(other.target))
            return false;
        return true;
    }
}
