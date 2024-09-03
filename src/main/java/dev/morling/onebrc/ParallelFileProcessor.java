package dev.morling.onebrc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class processes a large file in parallel, reading it in chunks and processing each chunk concurrently.
 */
public class ParallelFileProcessor {

    // The number of threads to use in the thread pool, based on the number of available processors
//    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 15;

    // The size of each chunk of the file to read and process
    private static final int CHUNK_SIZE = 1024 * 1024; // 1MB per chunk

    /**
     * The main entry point of the program.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        String filePath = "./measurements.txt"; // Replace with your file path
        String outPutFilePath = Path.of("out_chunk.txt").toString();
        ConcurrentHashMap<String, AtomicReference<ValueHolder>> map = new ConcurrentHashMap<>();
        List<Long[]> bufferPositions = new ArrayList<>();
        // Create a thread pool with the specified number of threads
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

        try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            long fileSize = fileChannel.size();
            System.out.println("fileSize: " + fileSize);
            final long[] positionStart = {0};
            final long[] positionEnd = {0};

            // Read and process the file in chunks
            while (positionEnd[0] < fileSize) {
                positionStart[0] = positionEnd[0];
                long chunkSize = Math.min(CHUNK_SIZE, fileSize - positionEnd[0]);

                positionEnd[0] += chunkSize;
                // Ensure that the chunk ends at a newline character
                ByteBuffer bufferAtEndOfChunk = ByteBuffer.allocate(1);
                boolean isAtEndOfChunkIsEndOfLine = positionEnd[0] == fileSize;

                while (!isAtEndOfChunkIsEndOfLine && positionEnd[0] < fileSize) {
                    try {
                        fileChannel.read(bufferAtEndOfChunk, positionEnd[0]);
                    } catch (IOException e) {
                        System.out.println(e.getMessage());
                    }
                    bufferAtEndOfChunk.flip();
                    if (bufferAtEndOfChunk.get() == '\n') {
                        isAtEndOfChunkIsEndOfLine = true;
                        positionEnd[0]++;
                        break;
                    } else {
                        bufferAtEndOfChunk.clear();
                        positionEnd[0]++;
                    }
                }
                bufferPositions.add(new Long[]{positionStart[0], Math.min(positionEnd[0], fileSize)});
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            openAndReadFileByChunk(filePath, bufferPositions, executorService, map);
            // Shut down the thread pool
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
                    System.out.println("Thread pool shutdown fail. force shut down");
                    executorService.shutdownNow();
                } else {
                    System.out.println("Thread pool shutdown successfully");
                    System.out.println("Thời gian thực thi read and process: " + (System.currentTimeMillis() - startTime) + " milliseconds");

                    // Convert the map to a sorted list
                    writeResultsToFile(map, outPutFilePath);

                    // Kết thúc tính thời gian
                    long endTime = System.currentTimeMillis();

                    // Tính toán thời gian thực thi
                    long executionTime = endTime - startTime;
                    System.out.println("Thời gian thực thi: " + executionTime + " milliseconds");
                }

            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }


        }


    }

    private static void openAndReadFileByChunk(String filePath, List<Long[]> bufferPositions, ExecutorService executorService, ConcurrentHashMap<String, AtomicReference<ValueHolder>> map) {
        try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath), StandardOpenOption.READ)) {
            bufferPositions.stream().parallel().forEach(bufferPosition -> {
                ByteBuffer buffer = null;
                try {
                    buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, bufferPosition[0], bufferPosition[1] - bufferPosition[0]);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
                ByteBuffer finalBuffer = buffer;
                executorService.submit(() -> processChunk(finalBuffer, map, bufferPosition[0], bufferPosition[1]));
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeResultsToFile(ConcurrentHashMap<String, AtomicReference<ValueHolder>> map, String outPutFilePath) {
        List<Map.Entry<String, AtomicReference<ValueHolder>>> list = new ArrayList<>(map.entrySet().stream().parallel().toList());
        list.sort(Map.Entry.comparingByKey());


        // Write the list to a file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outPutFilePath))) {
            for (Map.Entry<String, AtomicReference<ValueHolder>> entry : list) {
                writer.write(new StringBuilder().append(entry.getKey()).append(";").append(entry.getValue().get().max).append(";").append(entry.getValue().get().min).append(";").append((entry.getValue().get().sumForMean)).append(";").append(entry.getValue().get().countAsync).toString());
                writer.newLine();  // Tạo một dòng mới
            }
            System.out.println("Đã ghi Map vào file: " + outPutFilePath);
        } catch (IOException e) {
            System.out.println("Somethinng go wrong!");
            e.printStackTrace();
        }
    }

    /**
     * Process a chunk of the file.
     *
     * @param buffer the chunk of the file as a ByteBuffer
     * @param map    the map to store the processed data in
     */
    private static void processChunk(ByteBuffer buffer, ConcurrentHashMap<String, AtomicReference<ValueHolder>> map, long positionStart, long positionEnd) {
        System.out.println("Processing chunk: " + positionStart + " - " + positionEnd);
        List<String> lines = List.of(StandardCharsets.UTF_8.decode(buffer).toString().split("\n"));
        lines.stream().parallel().forEach(l -> processLine(l, map));
    }

    /**
     * Process a line of the file.
     *
     * @param part the line to process
     * @param map  the map to store the processed data in
     */
    private static void processLine(String part, ConcurrentHashMap<String, AtomicReference<ValueHolder>> map) {
        // Custom processing logic for each line
        String[] parts = part.split(";");
        if (parts.length != 2) {
            System.out.println("line invalid: " + part);
            return;
        }
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


