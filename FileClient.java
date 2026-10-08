import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;

/**
 * Multi-threaded File Download Client
 *
 * วิธีรัน:
 *   java FileClient <host> <port> list
 *   java FileClient <host> <port> info <file>
 *   java FileClient <host> <port> download <file> [io|nio] [workers=10] [repeat=1] [outDir=downloads]
 *
 * แต่ละ worker: connection ของตัวเอง + offset/length ของตัวเอง + file handle/channel ของตัวเอง
 * เขียนลง "final file" ตาม offset โดยตรง (ไม่ต้อง merge part files)
 */
public class FileClient {

    record Range(int id, long offset, long length) {}

    public static void main(String[] a) throws Exception {
        if (a.length < 3) { usage(); return; }
        String host = a[0];
        int port = Integer.parseInt(a[1]);
        switch (a[2].toLowerCase()) {
            case "list" -> {
                try (Conn c = new Conn(host, port)) {
                    c.send("LIST");
                    String l;
                    while (!(l = c.readLine()).equals("END")) System.out.println(l);
                }
            }
            case "info" -> {
                try (Conn c = new Conn(host, port)) {
                    c.send("INFO " + a[3]);
                    System.out.println(c.readLine());
                }
            }
            case "download" -> {
                String file = a[3];
                boolean nio = a.length > 4 && a[4].equalsIgnoreCase("nio");
                int workers = a.length > 5 ? Integer.parseInt(a[5]) : 10;
                int repeat = a.length > 6 ? Integer.parseInt(a[6]) : 1;
                Path outDir = Path.of(a.length > 7 ? a[7] : "downloads");
                Files.createDirectories(outDir);
                benchmark(host, port, file, nio, workers, repeat, outDir);
            }
            default -> usage();
        }
    }

    static void usage() {
        System.err.println("""
            Usage:
              java FileClient <host> <port> list
              java FileClient <host> <port> info <file>
              java FileClient <host> <port> download <file> [io|nio] [workers=10] [repeat=1] [outDir=downloads]""");
    }

    // ------------------------------------------------------------------
    //  ดาวน์โหลด (อาจซ้ำหลายรอบเพื่อวัดผล) + ตรวจ size / SHA-256
    // ------------------------------------------------------------------
    static void benchmark(String host, int port, String file, boolean nio, int workers, int repeat, Path outDir) throws Exception {
        // 1) ขอขนาดไฟล์ และ hash ที่ถูกต้องจาก server (ยังไม่จับเวลา)
        long size; String expectedHash;
        try (Conn c = new Conn(host, port)) {
            c.send("INFO " + file);
            String r = c.readLine();
            if (!r.startsWith("SIZE ")) throw new IOException("Server replied: " + r);
            size = Long.parseLong(r.substring(5));
            c.send("HASH " + file);
            r = c.readLine();
            if (!r.startsWith("HASH ")) throw new IOException("Server replied: " + r);
            expectedHash = r.substring(5);
        }
        String mode = nio ? "nio" : "io";
        System.out.printf("File=%s size=%d bytes (%.2f MB) mode=%s workers=%d repeat=%d%n",
                file, size, size / 1_048_576.0, mode, workers, repeat);

        List<Range> ranges = computeRanges(size, workers);
        for (Range r : ranges) System.out.printf("  worker %2d : offset=%d length=%d%n", r.id(), r.offset(), r.length());

        double[] secs = new double[repeat];
        for (int run = 1; run <= repeat; run++) {
            Path out = outDir.resolve(file);
            Files.deleteIfExists(out);

            long t0 = System.nanoTime();
            downloadOnce(host, port, file, size, ranges, nio, out);
            double sec = (System.nanoTime() - t0) / 1e9;            // เวลารวม (ไม่รวมเวลา hash)

            boolean sizeOk = Files.size(out) == size;
            boolean hashOk = sha256(out).equals(expectedHash);
            double mbps = size / 1_048_576.0 / sec;
            secs[run - 1] = sec;
            System.out.printf("run %d: time=%.3f s  throughput=%.2f MB/s  size=%s  sha256=%s%n",
                    run, sec, mbps, sizeOk ? "OK" : "MISMATCH", hashOk ? "OK" : "MISMATCH");
            System.out.printf("RESULT,%s,%d,%d,%.3f,%.2f,%s%n", mode, workers, run, sec, mbps, (sizeOk && hashOk) ? "OK" : "FAIL");
        }
        double avg = Arrays.stream(secs).average().orElse(0);
        System.out.printf("SUMMARY mode=%s workers=%d  avg=%.3f s (%.2f MB/s)  min=%.3f s  max=%.3f s%n",
                mode, workers, avg, size / 1_048_576.0 / avg,
                Arrays.stream(secs).min().orElse(0), Arrays.stream(secs).max().orElse(0));
    }

    /** แบ่งไฟล์เป็น n ช่วงไม่ซ้อนกัน; worker สุดท้ายรับส่วนที่เหลือ (เศษจากการหาร) */
    static List<Range> computeRanges(long size, int n) {
        List<Range> list = new ArrayList<>();
        long chunk = size / n;
        for (int i = 0; i < n; i++) {
            long off = i * chunk;
            long len = (i == n - 1) ? size - off : chunk;
            list.add(new Range(i, off, len));
        }
        return list;
    }

    static void downloadOnce(String host, int port, String file, long size, List<Range> ranges, boolean nio, Path out) throws Exception {
        // สร้าง final file ขนาดเท่าต้นฉบับไว้ก่อน เพื่อให้ทุก worker เขียนตาม offset ของตัวเองได้
        // ใช้ SPARSE + เขียน 1 byte ท้ายไฟล์ แทน setLength(): บน Windows/NTFS การเขียนไกลเกินส่วนที่เคยเขียนจริง
        // จะทำให้ OS ต้อง "เติมศูนย์" ช่วงที่ข้ามไปก่อน ทำให้หลาย worker ที่เขียนคนละ offset ช้ามาก
        try (FileChannel fc = FileChannel.open(out, StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE, StandardOpenOption.SPARSE)) {
            if (size > 0) fc.write(java.nio.ByteBuffer.wrap(new byte[1]), size - 1);
        }
        ExecutorService pool = Executors.newFixedThreadPool(ranges.size());   // 1 worker ต่อ 1 ช่วง
        try {
            List<Future<Long>> futures = new ArrayList<>();
            for (Range r : ranges) {
                futures.add(pool.submit(() -> nio ? workerNio(host, port, file, r, out)
                                                  : workerTraditional(host, port, file, r, out)));
            }
            long total = 0;
            for (Future<Long> f : futures) {
                try { total += f.get(); }
                catch (ExecutionException e) { throw new IOException("worker failed: " + e.getCause(), e.getCause()); }
            }
            if (total != size) throw new IOException("received " + total + " bytes, expected " + size);
        } finally {
            pool.shutdownNow();
        }
    }

    // ------------------------------------------------------------------
    //  Worker แบบ Traditional I/O
    //  InputStream.read() -> byte[] -> RandomAccessFile.seek() + write()
    // ------------------------------------------------------------------
    static long workerTraditional(String host, int port, String file, Range r, Path out) throws IOException {
        if (r.length() == 0) return 0;
        try (Socket s = new Socket(host, port);
             RandomAccessFile raf = new RandomAccessFile(out.toFile(), "rw")) {   // file handle ของ worker นี้เอง
            InputStream in = new BufferedInputStream(s.getInputStream(), 64 * 1024);
            OutputStream os = s.getOutputStream();
            os.write(("GET " + file + " " + r.offset() + " " + r.length() + "\n").getBytes(StandardCharsets.UTF_8));
            os.flush();

            long len = parseOk(readLine(in), r);
            raf.seek(r.offset());
            byte[] buf = new byte[64 * 1024];
            long remaining = len;
            while (remaining > 0) {
                int n = in.read(buf, 0, (int) Math.min(buf.length, remaining));
                if (n < 0) throw new EOFException("connection closed early (worker " + r.id() + ")");
                raf.write(buf, 0, n);
                remaining -= n;
            }
            return len;
        }
    }

    // ------------------------------------------------------------------
    //  Worker แบบ NIO native transfer
    //  FileChannel.transferFrom(SocketChannel, position, count)
    // ------------------------------------------------------------------
    static long workerNio(String host, int port, String file, Range r, Path out) throws IOException {
        if (r.length() == 0) return 0;
        try (SocketChannel sc = SocketChannel.open(new InetSocketAddress(host, port));
             FileChannel fc = FileChannel.open(out, StandardOpenOption.WRITE)) {  // channel ของ worker นี้เอง
            OutputStream os = sc.socket().getOutputStream();
            os.write(("GET " + file + " " + r.offset() + " " + r.length() + "\n").getBytes(StandardCharsets.UTF_8));
            os.flush();

            // header อ่านแบบไม่ buffer เพื่อไม่กลืน byte ของ payload
            long len = parseOk(readLine(sc.socket().getInputStream()), r);
            long pos = r.offset(), remaining = len;
            while (remaining > 0) {
                long n = fc.transferFrom(sc, pos, remaining);   // เขียนที่ตำแหน่ง pos (positional) ไม่แตะ position กลาง
                if (n <= 0) throw new EOFException("connection closed early (worker " + r.id() + ")");
                pos += n;
                remaining -= n;
            }
            return len;
        }
    }

    // ------------------------------------------------------------------
    //  helpers
    // ------------------------------------------------------------------

    /** ตรวจ header "OK <len>" ; ถ้าเป็น ERROR ให้โยน exception พร้อมข้อความจาก server */
    static long parseOk(String header, Range r) throws IOException {
        if (header == null) throw new EOFException("no response");
        if (header.startsWith("ERROR")) throw new IOException("worker " + r.id() + ": " + header);
        if (!header.startsWith("OK ")) throw new IOException("bad response: " + header);
        long len = Long.parseLong(header.substring(3).trim());
        if (len != r.length()) throw new IOException("length mismatch: " + len + " != " + r.length());
        return len;
    }

    static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') return bo.toString(StandardCharsets.UTF_8).replace("\r", "");
            bo.write(b);
        }
        return bo.size() == 0 ? null : bo.toString(StandardCharsets.UTF_8);
    }

    static String sha256(Path f) throws IOException {
        try (InputStream is = Files.newInputStream(f)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[1 << 20];
            int n;
            while ((n = is.read(buf)) > 0) md.update(buf, 0, n);
            return HexFormat.of().formatHex(md.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    /** connection สำหรับคำสั่งสั้น ๆ (LIST/INFO/HASH) */
    static class Conn implements Closeable {
        final Socket s; final InputStream in; final OutputStream out;
        Conn(String host, int port) throws IOException {
            s = new Socket(host, port); in = s.getInputStream(); out = s.getOutputStream();
        }
        void send(String cmd) throws IOException { out.write((cmd + "\n").getBytes(StandardCharsets.UTF_8)); out.flush(); }
        String readLine() throws IOException { return FileClient.readLine(in); }
        public void close() throws IOException { s.close(); }
    }
}