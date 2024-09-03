package dev.morling.onebrc;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalculateAverageLuutkha {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String filePath = Path.of("./measurements.txt").toString();
        String outPutFilePath = Path.of("out.txt").toString();
        Map<String, ValueHolder> map = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(";");
                String city = parts[0];
                double valueOnDouble = Math.round(Double.parseDouble(parts[1]) * 10);
                int value = (int) valueOnDouble;

                ValueHolder valueHolder = new ValueHolder(value, value,value);
                if (map.containsKey(city)) {
                    valueHolder = map.get(city);
                    valueHolder.max = Math.max(valueHolder.max, value);
                    valueHolder.min = Math.min(valueHolder.min, value);
                    valueHolder.sumForMean += value  ;
                    valueHolder.count = valueHolder.count + 1;

                } else {
                    valueHolder.max = Math.max(valueHolder.max, value);
                    valueHolder.min = Math.min(valueHolder.min, value);
                    valueHolder.sumForMean = value;
                    valueHolder.count = 1;
                    map.put(city, valueHolder);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Map.Entry<String, ValueHolder>> list = new ArrayList<>(map.entrySet().stream().parallel().toList());
        list.sort(Map.Entry.comparingByKey());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outPutFilePath))) {
            for (Map.Entry<String, ValueHolder> entry : list) {
                StringBuilder sb = new StringBuilder();
                sb.append(entry.getKey()).append(";").append(entry.getValue().max).append(";").append(entry.getValue().min).append(";").append(entry.getValue().sumForMean).append(";").append(entry.getValue().count);
                writer.write(sb.toString());
                writer.newLine();  // Tạo một dòng mới
            }
            System.out.println("Đã ghi Map vào file: " + outPutFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Kết thúc tính thời gian
        long endTime = System.currentTimeMillis();

        // Tính toán thời gian thực thi
        long executionTime = endTime - startTime;
        System.out.println("Thời gian thực thi: " + executionTime + " milliseconds");
    }
}

