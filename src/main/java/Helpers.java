import java.util.*;
import java.util.stream.Collectors;

public class Helpers {

    //returns an iterator over a large range. Using Iterator to keep track of pID, and not assign same pID to multiple processes
    public static Iterator range_iterator(int start, int length) {
        ArrayList<String> range = new ArrayList<>();

        for (int i = start; i <= length; i++) {
            range.add(Integer.toString(i - start));
        }
        Iterator range_iter = range.iterator();
        return range_iter;
    }

    //reverse sort a hashmap by values
    public static <String, Integer extends Comparable<? super Integer>> Map<String, Integer> sortByValue(Map<String, Integer> map) {
        return map.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (e1, e2) -> e1,
                        LinkedHashMap::new
                ));
    }
}
