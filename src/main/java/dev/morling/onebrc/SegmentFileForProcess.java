package dev.morling.onebrc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SegmentFileForProcess {
    private static long findSegment(int i, int skipSegment, RandomAccessFile raf, long location, long fileSize) throws IOException {
        if (i != skipSegment) {
            raf.seek(location);
            while (location < fileSize) {
                location++;
                if (raf.read() == '\n')
                    break;
            }
        }
        return location;
    }
    private static List<FileSegment> getFileSegments(File file) throws IOException {
        int numberOfSegments = Runtime.getRuntime().availableProcessors();
        long fileSize = file.length();
        long segmentSize = fileSize / numberOfSegments;
        List<FileSegment> segments = new ArrayList<>(numberOfSegments);
        // Pointless to split small files
        if (segmentSize < 1_000_000) {
            segments.add(new FileSegment(0, fileSize));
            return segments;
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            for (int i = 0; i < numberOfSegments; i++) {
                long segStart = i * segmentSize;
                long segEnd = (i == numberOfSegments - 1) ? fileSize : segStart + segmentSize;
                segStart = findSegment(i, 0, randomAccessFile, segStart, segEnd);
                segEnd = findSegment(i, numberOfSegments - 1, randomAccessFile, segEnd, fileSize);

                segments.add(new FileSegment(segStart, segEnd));
            }
        }
        return segments;
    }
    public static void main(String[] args) throws IOException {

        long startTime = System.currentTimeMillis();
        String filePath = Path.of("././measurements.txt").toString();
        String outPutFilePath = Path.of("out.txt").toString();
        Map<String, ValueHolder> map = new HashMap<>();

        getFileSegments(new File("./measurements.txt")).forEach(System.out::println);

    }


}
