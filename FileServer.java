import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

/**
 * Multi-threaded File Download Server (TCP)
 *
 * วิธีรัน:  java FileServer <port> <shareDir> [io|nio] [virtual|<จำนวน thread ใน pool>]
 *   io  = Traditional I/O : RandomAccessFile.read() + OutputStream.write()
 *   nio = NIO native      : FileChannel.transferTo(...) -> SocketChannel (sendfile)
 *
 * Protocol (text 1 บรรทัดต่อ 1 คำสั่ง, จบด้วย \n ; payload เป็น binary):
 *   LIST                    -> FILE <name> <size>\n ... END\n
 *   INFO <file>             -> SIZE <bytes>\n | ERROR <code> <msg>\n
 *   HASH <file>             -> HASH <sha256-hex>\n | ERROR ...
 *   GET <file> <off> <len>  -> OK <len>\n + payload(<len> bytes) | ERROR <code> <msg>\n
 *   QUIT                    -> BYE\n (แล้วปิด connection)
 * ERROR code: 400 = request ผิดรูปแบบ, 404 = ไม่พบไฟล์, 416 = ช่วง offset/length ไม่ถูกต้อง, 500 = server error
 */
public class FileServer {
    static final int BUF_SIZE = 64 * 1024;
    static Path root;
    static boolean nio;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java FileServer <port> <shareDir> [io|nio] [virtual|N]");
            return;
        }
        int port = Integer.parseInt(args[0]);
        root = Path.of(args[1]).toAbsolutePath().normalize();
        nio = args.length > 2 && args[2].equalsIgnoreCase("nio");
        String threadOpt = args.length > 3 ? args[3] : "virtual";

        // ----- เลือกโมเดล concurrency: 1 connection = 1 task -----
        ExecutorService pool = threadOpt.equalsIgnoreCase("virtual")
                ? Executors.newVirtualThreadPerTaskExecutor()          // Java 21+
                : Executors.newFixedThreadPool(Integer.parseInt(threadOpt));

        System.out.printf("Server start port=%d dir=%s mode=%s threads=%s%n",
                port, root, nio ? "NIO(transferTo)" : "Traditional I/O", threadOpt);

        if (nio) {
            try (ServerSocketChannel ssc = ServerSocketChannel.open()) {
                ssc.bind(new InetSocketAddress(port));                    // blocking mode
                while (true) {
                    SocketChannel sc = ssc.accept();
                    pool.submit(() -> {
                        try {
                            return serve(sc.socket().getInputStream(), sc.socket().getOutputStream(), sc, sc);
                        } catch (IOException e) {
                            System.err.println("[conn] " + e);
                            try { sc.close(); } catch (IOException ignore) { }
                            return null;
                        }
                    });
                }
            }
        } else {
            try (ServerSocket ss = new ServerSocket(port)) {
                while (true) {
                    Socket s = ss.accept();
                    pool.submit(() -> {
                        try {
                            return serve(s.getInputStream(), s.getOutputStream(), null, s);
                        } catch (IOException e) {
                            System.err.println("[conn] " + e);
                            try { s.close(); } catch (IOException ignore) { }
                            return null;
                        }
                    });
                }
            }
        }
    }   // <- ปีกกาปิดของ main

    /** loop รับคำสั่งจาก 1 connection จนกว่า client จะปิด / QUIT */
    static Void serve(InputStream in, OutputStream out, SocketChannel ch, Closeable conn) {
        try (conn) {
            String line;
            while ((line = readLine(in)) != null) {
                String[] p = line.trim().split("\\s+");
                switch (p[0].toUpperCase()) {
                    case "LIST" -> list(out);
                    case "INFO" -> info(p, out);
                    case "HASH" -> hash(p, out);
                    case "GET"  -> get(p, in, out, ch);
                    case "QUIT" -> { send(out, "BYE\n"); return null; }
                    default     -> send(out, "ERROR 400 Unknown command\n");
                }
            }
        } catch (IOException e) {
            System.err.println("[conn] " + e);
        }
        return null;
    }

    // ---------------- command handlers ----------------

    static void list(OutputStream out) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (Stream<Path> s = Files.list(root)) {
            for (Path f : (Iterable<Path>) s.filter(Files::isRegularFile).sorted()::iterator) {
                String name = f.getFileName().toString();
                if (name.contains(" ")) continue;      // protocol แยกด้วย space จึงข้ามชื่อที่มีช่องว่าง
                sb.append("FILE ").append(name).append(' ').append(Files.size(f)).append('\n');
            }
        }
        sb.append("END\n");
        send(out, sb.toString());
    }

    static void info(String[] p, OutputStream out) throws IOException {
        if (p.length != 2) { send(out, "ERROR 400 Usage: INFO <filename>\n"); return; }
        Path f = resolve(p[1]);
        if (f == null) { send(out, "ERROR 404 File not found\n"); return; }
        send(out, "SIZE " + Files.size(f) + "\n");
    }

    static void hash(String[] p, OutputStream out) throws IOException {
        if (p.length != 2) { send(out, "ERROR 400 Usage: HASH <filename>\n"); return; }
        Path f = resolve(p[1]);
        if (f == null) { send(out, "ERROR 404 File not found\n"); return; }
        send(out, "HASH " + sha256(f) + "\n");
    }

    static void get(String[] p, InputStream in, OutputStream out, SocketChannel ch) throws IOException {
        if (p.length != 4) { send(out, "ERROR 400 Usage: GET <filename> <offset> <length>\n"); return; }
        long off, len;
        try { off = Long.parseLong(p[2]); len = Long.parseLong(p[3]); }
        catch (NumberFormatException e) { send(out, "ERROR 400 offset/length must be numbers\n"); return; }
        Path f = resolve(p[1]);
        if (f == null) { send(out, "ERROR 404 File not found\n"); return; }
        long size = Files.size(f);
        // ตรวจช่วงข้อมูล: ไม่ติดลบ และไม่เกินท้ายไฟล์ (ใช้ลบแทนบวกเพื่อกัน overflow)
        if (off < 0 || len < 0 || off > size || len > size - off) {
            send(out, "ERROR 416 Invalid range (file size=" + size + ")\n");
            return;
        }
        send(out, "OK " + len + "\n");                   // header บอกจำนวน byte ของ payload
        if (nio) sendNio(f, off, len, ch); else sendTraditional(f, off, len, out);
    }

    // ---------------- 2 วิธีส่งข้อมูล ----------------

    /** Traditional I/O: อ่านเข้า byte[] ใน user space แล้วเขียนลง socket */
    static void sendTraditional(Path f, long off, long len, OutputStream out) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "r")) {
            raf.seek(off);
            byte[] buf = new byte[BUF_SIZE];
            long remaining = len;
            while (remaining > 0) {
                int n = raf.read(buf, 0, (int) Math.min(buf.length, remaining));
                if (n < 0) throw new EOFException("file shrank while sending");
                out.write(buf, 0, n);
                remaining -= n;
            }
            out.flush();
        }
    }

    /** NIO: ให้ kernel ย้ายข้อมูล file -> socket โดยตรง (zero-copy / sendfile บน Linux) */
    static void sendNio(Path f, long off, long len, SocketChannel ch) throws IOException {
        try (FileChannel fc = FileChannel.open(f, java.nio.file.StandardOpenOption.READ)) {
            long pos = off, remaining = len;
            int zeros = 0;
            while (remaining > 0) {                      // transferTo อาจส่งได้น้อยกว่าที่ขอ จึงต้องวน loop
                long n = fc.transferTo(pos, remaining, ch);
                if (n == 0) {
                    if (pos >= fc.size()) throw new EOFException("file shrank while sending");
                    if (++zeros > 100000) throw new IOException("transferTo stuck (buffer full)");
                    Thread.yield();                      // คืนคิว CPU ให้ Thread อื่นทำงานก่อน
                    continue;
                }
                zeros = 0;                               // รีเซ็ตตัวนับเมื่อส่งสำเร็จ
                pos += n;
                remaining -= n;
            }
        }
    }

    // ---------------- helpers ----------------

    /** แปลงชื่อไฟล์เป็น Path ใน shareDir เท่านั้น (กัน path traversal เช่น ../../etc/passwd) */
    static Path resolve(String name) {
        if (name.isEmpty() || name.contains("/") || name.contains("\\") || name.equals("..") || name.equals(".")) return null;
        Path f = root.resolve(name).normalize();
        return (f.getParent() != null && f.getParent().equals(root) && Files.isRegularFile(f)) ? f : null;
    }

    static void send(OutputStream out, String s) throws IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** อ่านทีละ byte จนเจอ \n (ไม่ buffer เกิน เพื่อไม่ไปกลืน byte ของข้อมูลถัดไป) */
    static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') return bo.toString(StandardCharsets.UTF_8).replace("\r", "");
            if (bo.size() > 8192) throw new IOException("line too long");
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
}