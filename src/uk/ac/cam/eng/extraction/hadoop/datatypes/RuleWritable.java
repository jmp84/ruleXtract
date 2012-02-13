/**
 * 
 */

package uk.ac.cam.eng.extraction.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import uk.ac.cam.eng.extraction.datatypes.Rule;

/**
 * @author jmp84 This class represents a writable rule, it's essentially the
 *         same as a Rule but is convenient to be used in the map-reduced
 *         framework
 */
public class RuleWritable implements WritableComparable<RuleWritable> {

    private Text leftHandSide;
    private Text source;
    private Text target;
    
    // additional info for features
    private DoubleWritable numberUnalignedSourceWords;
    private DoubleWritable numberUnalignedTargetWords;

    public Text getLeftHandSide() {
        return leftHandSide;
    }

    /**
     * @param leftHandSide
     *            the leftHandSide to set
     */
    public void setLeftHandSide(Text leftHandSide) {
        this.leftHandSide = leftHandSide;
    }

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

	/**
	 * @return the numberUnalignedSourceWords
	 */
	public DoubleWritable getNumberUnalignedSourceWords() {
		return numberUnalignedSourceWords;
	}

	/**
	 * @param numberUnalignedSourceWords the numberUnalignedSourceWords to set
	 */
	public void setNumberUnalignedSourceWords(
			DoubleWritable numberUnalignedSourceWords) {
		this.numberUnalignedSourceWords = numberUnalignedSourceWords;
	}

	/**
	 * @return the numberUnalignedTargetWords
	 */
	public DoubleWritable getNumberUnalignedTargetWords() {
		return numberUnalignedTargetWords;
	}

	/**
	 * @param numberUnalignedTargetWords the numberUnalignedTargetWords to set
	 */
	public void setNumberUnalignedTargetWords(
			DoubleWritable numberUnalignedTargetWords) {
		this.numberUnalignedTargetWords = numberUnalignedTargetWords;
	}

	public RuleWritable() {
        leftHandSide = new Text();
        source = new Text();
        target = new Text();
        numberUnalignedSourceWords = new DoubleWritable();
        numberUnalignedTargetWords = new DoubleWritable();
    }

    public RuleWritable(Rule r) {
        String[] parts = r.toString().split("\\s+");
        leftHandSide = new Text(parts[0]);
        source = new Text(parts[1]);
        if (parts.length == 3) {
            target = new Text(parts[2]);
        }
        else {
            target = new Text();
        }
        numberUnalignedSourceWords = new DoubleWritable(r.getNumberUnalignedSourceWords());
        numberUnalignedTargetWords = new DoubleWritable(r.getNumberUnalignedTargetWords());
    }

    public RuleWritable(RuleWritable source, RuleWritable target) {
        leftHandSide = source.leftHandSide;
        this.source = source.source;
        this.target = target.target;
        this.numberUnalignedSourceWords = target.numberUnalignedSourceWords;
        this.numberUnalignedTargetWords = target.numberUnalignedTargetWords;
    }

    public static RuleWritable makeSourceMarginal(Rule r) {
        String[] parts = r.toString().split("\\s+");
        RuleWritable res = new RuleWritable();
        res.leftHandSide = new Text(parts[0]);
        res.source = new Text(parts[1]);
        res.target = new Text();
        return res;
    }

    public static RuleWritable makeSourceMarginal(RuleWritable r) {
        RuleWritable res = new RuleWritable();
        res.leftHandSide = new Text(r.leftHandSide);
        res.source = new Text(r.source);
        res.target = new Text();
        return res;
    }

    public static RuleWritable makeTargetMarginal(Rule r) {
        String[] parts = r.toString().split("\\s+");
        RuleWritable res = new RuleWritable();
        res.leftHandSide = new Text(parts[0]);
        res.target = new Text(parts[2]);
        res.source = new Text();
        res.numberUnalignedSourceWords = new DoubleWritable(r.getNumberUnalignedSourceWords());
        res.numberUnalignedTargetWords = new DoubleWritable(r.getNumberUnalignedTargetWords());
        return res;
    }

    public static RuleWritable makeTargetMarginal(RuleWritable r) {
        RuleWritable res = new RuleWritable();
        res.leftHandSide = new Text(r.leftHandSide);
        res.source = new Text();
        res.target = new Text(r.target);
        res.numberUnalignedSourceWords = r.numberUnalignedSourceWords;
        res.numberUnalignedTargetWords = r.numberUnalignedTargetWords;
        return res;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leftHandSide);
        sb.append(" ");
        sb.append(source);
        sb.append(" ");
        sb.append(target);
        return sb.toString();
    }

    /**
     * Prints a rule as found in shallow grammar (.lex.gz)
     * 
     * @return
     */
    public String toStringShallow() {
        Rule r = new Rule(this);
        // glue rules
        if (r.isConcatenatingGlue()) {
            return "S S_X S_X";
        }
        if (r.isStartSentence()) {
            return "X 1 <s><s><s>";
        }
        if (r.isEndSentence()) {
            return "X 2 </s>";
        }
        if (r.isStartingGlue()) {
            return "X V V";
        }
        // deletion, oov, ascii rules
        if (r.isDeletion()) {
            return "X " + source.toString() + " <dr>";
        }
        if (r.isOov()) {
            return "X " + source.toString() + " <oov>";
        }
        if (r.isAscii()) {
            return "X " + source.toString() + " " + target.toString();
        }
        // TODO finish this
        return "";
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
     */
    @Override
    public void readFields(DataInput arg0) throws IOException {
        leftHandSide.readFields(arg0);
        source.readFields(arg0);
        target.readFields(arg0);
        numberUnalignedSourceWords.readFields(arg0);
        numberUnalignedTargetWords.readFields(arg0);
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
     */
    @Override
    public void write(DataOutput arg0) throws IOException {
        leftHandSide.write(arg0);
        source.write(arg0);
        target.write(arg0);
        numberUnalignedSourceWords.write(arg0);
        numberUnalignedTargetWords.write(arg0);
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(RuleWritable arg0) {
        int cmp = leftHandSide.compareTo(arg0.leftHandSide);
        if (cmp != 0)
            return cmp;
        cmp = source.compareTo(arg0.source);
        if (cmp != 0)
            return cmp;
        cmp = target.compareTo(arg0.target);
        if (cmp != 0)
        	return cmp;
        cmp = numberUnalignedSourceWords.compareTo(arg0.numberUnalignedSourceWords);
        if (cmp != 0)
        	return cmp;
        return numberUnalignedTargetWords.compareTo(arg0.numberUnalignedTargetWords);
    }

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((leftHandSide == null) ? 0 : leftHandSide.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((target == null) ? 0 : target.hashCode());
		return result;
	}

	// for the equals and hashcode methods, we don't use the
	// numberUnalignedSourceWords and numberUnalignedTargetWords
	// fields because in a hash map we group together rules
	// that come from different alignments
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		RuleWritable other = (RuleWritable) obj;
		if (leftHandSide == null) {
			if (other.leftHandSide != null) {
				return false;
			}
		} else if (!leftHandSide.equals(other.leftHandSide)) {
			return false;
		}
		if (source == null) {
			if (other.source != null) {
				return false;
			}
		} else if (!source.equals(other.source)) {
			return false;
		}
		if (target == null) {
			if (other.target != null) {
				return false;
			}
		} else if (!target.equals(other.target)) {
			return false;
		}
		return true;
	}
}
