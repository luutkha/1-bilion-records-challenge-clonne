package dev.morling.onebrc;

import java.io.*;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_luutkha2 {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String filePath = Path.of("./measurements.txt").toString();
        String outPutFilePath = Path.of("out.txt").toString();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            TreeMap<String, CityData> cityDataMap = new TreeMap<>();
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(";");
                String city = parts[0];
                double value = Double.parseDouble(parts[1]);

                cityDataMap.computeIfAbsent(city, k -> new CityData()).addValue(value);
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outPutFilePath))) {
                cityDataMap.forEach((city, data) -> {
                    StringBuilder sb = new StringBuilder();
                    sb.append(city).append(";").append(data.getMax()).append(";").append(data.getMin()).append(";").append(data.getMean());
                    try {
                        writer.write(sb.toString());
                        writer.newLine();  // Tạo một dòng mới
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                System.out.println("Đã ghi Map vào file: " + outPutFilePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Kết thúc tính thời gian
        long endTime = System.currentTimeMillis();

        // Tính toán thời gian thực thi
        long executionTime = endTime - startTime;
        System.out.println("Thời gian thực thi: " + executionTime + " milliseconds");
    }

    private static class CityData {
        private double maxValue;
        private double minValue;
        private AtomicLong sum = new AtomicLong();
        private AtomicLong count = new AtomicLong();

        public void addValue(double value) {
            maxValue = Math.max(maxValue, value);
            minValue = Math.min(minValue, value);
            sum.addAndGet((long) value);
            count.incrementAndGet();
        }

        public double getMax() {
            return maxValue;
        }

        public double getMin() {
            return minValue;
        }

        public double getMean() {
            return sum.get() / count.get();
        }
    }
}