package dev.morling.onebrc;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class BufferReaderAndParallelProcess {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String filePath = Path.of("./measurements.txt").toString();
        String outPutFilePath = Path.of("out.txt").toString();
        Map<String, AtomicReference<ValueHolder>> map = new ConcurrentHashMap<>();
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        int availableCores = Runtime.getRuntime().availableProcessors();

        Semaphore semaphore = new Semaphore(250); // Giới hạn 100 task đồng thời

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String finalLine = line;
                semaphore.acquire(); // Giảm số lượng permit khi một task bắt đầu
                try{

                    executorService.submit(() -> processLine(finalLine, map));
                }
                finally {
                    semaphore.release();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();

            List<Map.Entry<String, AtomicReference<ValueHolder>>> list = new ArrayList<>(map.entrySet().stream().parallel().toList());
            list.sort(Map.Entry.comparingByKey());
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outPutFilePath))) {
                for (Map.Entry<String, AtomicReference<ValueHolder>> entry : list) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(entry.getKey()).append(";").append(entry.getValue().get().max).append(";").append(entry.getValue().get().min).append(";").append(entry.getValue().get().sumForMean).append(";").append(entry.getValue().get().count);
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

    private static void processLine(String line, Map<String, AtomicReference<ValueHolder>> map) {
        // Custom processing logic for each line
        String[] parts = line.split(";");

        String city = parts[0];
        double valueOnDouble = Math.round(Double.parseDouble(parts[1]) * 10);
        int value = (int) valueOnDouble;

        map.computeIfAbsent(city, k -> new AtomicReference<>(new ValueHolder(value, value, new AtomicInteger(0), value)));

        ValueHolder updatedValueHolder;
        boolean isUpdated = false;
        do {
            updatedValueHolder = map.get(city).get();
            ValueHolder newUpdatedValueHolder = new ValueHolder(Math.max(updatedValueHolder.max, value), Math.min(updatedValueHolder.min, value), new AtomicInteger(updatedValueHolder.getCountAsync() + 1), updatedValueHolder.countAsync.get() == 0 ? value : updatedValueHolder.sumForMean + value);
            if (map.get(city).compareAndSet(updatedValueHolder, newUpdatedValueHolder)) {
                isUpdated = true;
            }
        } while (!isUpdated);
    }
}


