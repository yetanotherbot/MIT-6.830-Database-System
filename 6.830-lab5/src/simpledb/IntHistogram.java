package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] hist;
    private int minV, maxV;
    private double width;
    private int total;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	hist = new int[buckets];
        minV = min;
        maxV = max;
        width = (max-min+0.) / buckets;
        total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        ++hist[bucketOf(v)];
        ++total;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int b = bucketOf(v);
        double count = 0;
        switch (op) {
            case GREATER_THAN:
                if (v >= maxV) return 0;
                if (v < minV) return 1;
                count = greaterThanInBucket(v) + sumUp(b + 1, hist.length);
                break;
            case GREATER_THAN_OR_EQ:
                if (v > maxV) return 0;
                if (v <= minV) return 1;
                count = greaterThanInBucket(v) + eqInBucket(v) + sumUp(b + 1, hist.length);
                break;
            case LESS_THAN:
                if (v <= minV) return 0;
                if (v > maxV) return 1;
                count = lessThanInBucket(v) + sumUp(0, b);
                break;
            case LESS_THAN_OR_EQ:
                if (v < minV) return 0;
                if (v >= maxV) return 1;
                count = lessThanInBucket(v)  + eqInBucket(v) + sumUp(0, b);
                break;
            case EQUALS:
                if (v > maxV || v < minV) return 0;
                count = eqInBucket(v);
                break;
            case NOT_EQUALS:
                if (v > maxV || v < minV) return 1;
                count = total - eqInBucket(v);
                break;
            default: return 0;
        }
        return count / total;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("min: ");
        sb.append(minV);
        sb.append(" max: ");
        sb.append(maxV);
        sb.append("\n");
        for (int i = 0; i < hist.length; ++i) {
            sb.append(minV + i * width);
            sb.append(" ~ ");
            sb.append(minV + (i + 1) * width);
            sb.append(" : ");
            sb.append(hist[i]);
            sb.append("\n");
        }
        return sb.toString();
    }

    private int sumUp(int begin, int end) {
        int sum = 0;
        for (int i = begin; i < end; ++i)
            sum += hist[i];
        return sum;
    }

    private int bucketOf(int v) {
        if (v == maxV) return hist.length - 1;
        return ((int) ((v - minV) / width));
    }

    private double lessThanInBucket(int v) {
        int bucket = bucketOf(v);
        return (v - bucket * width - minV) / width * hist[bucket];
    }

    private double eqInBucket(int v) {
        int bucket = bucketOf(v);
        if (width < 1) return hist[bucket];
        return hist[bucket] / width;
    }

    private double greaterThanInBucket(int v) {
        int bucket = bucketOf(v);
        return ((bucket + 1) * width + minV - v) / width * hist[bucket];
    }
}
