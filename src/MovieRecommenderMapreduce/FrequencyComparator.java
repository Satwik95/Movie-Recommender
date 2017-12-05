package MovieRecommenderMapreduce;

import java.util.Comparator;

public class FrequencyComparator implements Comparator{
    @Override
    public int compare(Object o1, Object o2) {
        int[] a = (int[])o1;
        int[] b = (int[])o2;
        return Integer.compare(b[0], a[0]);
    }
}
