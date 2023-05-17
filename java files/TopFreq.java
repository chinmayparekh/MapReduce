import java.io.*;
import java.util.*;

public class TopFreq {
    private static List<Map.Entry<String, Integer>> list;
    public static List<String> result = new ArrayList<>();

    public static void main(String[] args) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader("/home/chinmay/eclipse-workspace/Frequency/output_wiki_docfreq/part-r-00000"));
        Map<String, Integer> map = new HashMap<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\s+");
            String key = parts[0];
            int value = Integer.parseInt(parts[1]);
            map.put(key, value);
        }
        list = new ArrayList<>(map.entrySet());
        list.sort(Map.Entry.comparingByValue());
        
        int count = 0;
        for (Map.Entry<String, Integer> entry : list) {
        	if (entry.getValue() > 14 && count < 100) {
        		result.add(entry.getKey());
        		count++;
        	}
        }
    

       BufferedWriter writer = new BufferedWriter(new FileWriter("output.tsv"));
       for (String s : result) {
               writer.write(s + "\n");
       }
       writer.close();
    }
}
