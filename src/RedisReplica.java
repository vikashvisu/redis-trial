import java.io.*;
import java.net.*;
import java.util.*;

public class RedisReplica {
    private static HashMap<String, String[]> map = new HashMap<>();
    private static String master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static long master_repl_offset = 0;
    private static boolean isReplica = false;
    private static final Set<String> writeCmds = new HashSet<>(Arrays.asList("SET", "INCR"));
    private static final Map<Socket, Long> replicaOffsets = new HashMap<>(); // Track replica offsets
    private static final Set<OutputStream> replicaOutputs = new HashSet<>(); // Replica output streams

    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        int port = 6379;

        try {
            if (args.length > 0 && args[0].equals("--port")) {
                port = Integer.parseInt(args[1]);
            }
            if (args.length > 2 && args[2].equals("--replicaof")) {
                String[] masterInfo = args[3].split(" ");
                String masterHost = masterInfo[0];
                int masterPort = Integer.parseInt(masterInfo[1]);
                isReplica = true;
                new Thread(new MasterConnectionHandler(masterHost, masterPort, port)).start();
            }

            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket, isReplica)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            if (serverSocket != null) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        }
    }

    static class MasterConnectionHandler implements Runnable {
        private final String host;
        private final int port;
        private final int replicaPort;

        MasterConnectionHandler(String host, int port, int replicaPort) {
            this.host = host;
            this.port = port;
            this.replicaPort = replicaPort;
        }

        public void run() {
            try (Socket masterSocket = new Socket(host, port)) {
                InputStream in = masterSocket.getInputStream();
                OutputStream out = masterSocket.getOutputStream();
                String resp = "*1\r\n$4\r\nPING\r\n";
                out.write(resp.getBytes());
                out.flush();
                readLine(in);

                String listeningPort =
                        "*3\r\n" +
                                "$8\r\nREPLCONF\r\n" +
                                "$14\r\nlistening-port\r\n" +
                                "$" + String.valueOf(replicaPort).length() + "\r\n" + replicaPort + "\r\n";
                out.write(listeningPort.getBytes());
                out.flush();
                readLine(in);

                String capa =
                        "*3\r\n" +
                                "$8\r\nREPLCONF\r\n" +
                                "$4\r\ncapa\r\n" +
                                "$6\r\npsync2\r\n";
                out.write(capa.getBytes());
                out.flush();
                readLine(in);

                String psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                out.write(psync.getBytes());
                out.flush();
                readLine(in);

                String lenLine = readLine(in);
                int len = Integer.parseInt(lenLine.substring(1));
                byte[] rdb = new byte[len];
                int read = 0;
                while (read < len) {
                    int n = in.read(rdb, read, len - read);
                    if (n <= 0) throw new IOException("Failed to read RDB");
                    read += n;
                }

                while (true) {
                    List<String> command = parseRESP(in);
                    if (command != null && !command.isEmpty()) {
                        execCommand(command);
                    }
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b == -1) {
                if (baos.size() == 0) return null;
                throw new EOFException("Unexpected EOF while reading line");
            }
            if (b == '\r') {
                int next = in.read();
                if (next == -1) throw new EOFException();
                if (next == '\n') break;
                baos.write(b);
                baos.write(next);
            } else {
                baos.write(b);
            }
        }
        return new String(baos.toByteArray(), "UTF-8");
    }

    private static List<String> parseRESP(InputStream in) throws IOException {
        String firstLine = readLine(in);
        if (firstLine == null) return null;
        if (firstLine.startsWith("*")) {
            int argCount = Integer.parseInt(firstLine.substring(1));
            List<String> args = new ArrayList<>();
            for (int i = 0; i < argCount; i++) {
                String lenLine = readLine(in);
                if (!lenLine.startsWith("$")) {
                    throw new IOException("Invalid bulk string length line: " + lenLine);
                }
                int length = Integer.parseInt(lenLine.substring(1));
                if (length == -1) {
                    args.add(null);
                    readLine(in);
                    continue;
                }
                byte[] buf = new byte[length];
                int r = 0;
                while (r < length) {
                    int n = in.read(buf, r, length - r);
                    if (n <= 0) throw new IOException("Failed to read bulk string");
                    r += n;
                }
                args.add(new String(buf, "UTF-8"));
                readLine(in);
            }
            return args;
        }
        throw new IOException("Invalid RESP input: " + firstLine);
    }

    private static boolean isExpired(String val) {
        if (val == null || Long.parseLong(val) == -1) return false;
        long currentTime = System.currentTimeMillis();
        return currentTime >= Long.parseLong(val);
    }

    private static boolean isInteger(String s) {
        if (s == null) return false;
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean set(List<String> command) {
        try {
            String key = command.get(1);
            String value = "0";
            if (command.size() > 2) {
                value = command.get(2);
            }
            long expTime = -1;
            if (command.size() > 3 && command.get(3).equalsIgnoreCase("px")) {
                expTime = System.currentTimeMillis() + Long.parseLong(command.get(4));
            }
            map.put(key, new String[]{value, String.valueOf(expTime)});
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String execCommand(List<String> command) {
        try {
            String cmd = command.get(0).toUpperCase();
            if (cmd.equals("PING")) {
                return "+PONG\r\n";
            } else if (cmd.equals("ECHO")) {
                String arg = command.get(1);
                return "$" + arg.length() + "\r\n" + arg + "\r\n";
            } else if (cmd.equals("SET")) {
                if (set(command)) {
                    return "+OK\r\n";
                } else {
                    return "-ERR invalid arguments\r\n";
                }
            } else if (cmd.equals("INCR")) {
                String key = command.get(1);
                if (!map.containsKey(key)) {
                    if (!set(command)) return "-ERR invalid arguments\r\n";
                }
                String[] values = map.get(key);
                String val = values[0];
                if (isInteger(val)) {
                    String newVal = String.valueOf(Integer.parseInt(val) + 1);
                    map.put(key, new String[]{newVal, values[1]});
                    return ":" + newVal + "\r\n";
                } else {
                    return "-ERR value is not an integer or out of range\r\n";
                }
            } else if (cmd.equals("GET")) {
                String key = command.get(1);
                if (map.containsKey(key)) {
                    String[] values = map.get(key);
                    if (isExpired(values[1])) return "$-1\r\n";
                    return "$" + values[0].length() + "\r\n" + values[0] + "\r\n";
                } else {
                    return "$-1\r\n";
                }
            } else if (cmd.equals("WAIT")) {
                int numReplicas = Integer.parseInt(command.get(1));
                long timeoutMs = Long.parseLong(command.get(2));
                long requiredOffset = master_repl_offset; // Offset of all prior writes
                long startTime = System.currentTimeMillis();
                int ackCount = 0;

                while (System.currentTimeMillis() - startTime < timeoutMs) {
                    synchronized (replicaOffsets) {
                        ackCount = 0;
                        for (Long offset : replicaOffsets.values()) {
                            if (offset >= requiredOffset) {
                                ackCount++;
                            }
                        }
                        if (ackCount >= numReplicas) {
                            break;
                        }
                    }
                    try {
                        Thread.sleep(10); // Poll every 10ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return ":" + ackCount + "\r\n"; // Return number of replicas that acknowledged
            } else {
                return null;
            }
        } catch (Exception e) {
            System.out.println("Error executing command: " + e.getMessage());
            return null;
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket socket;
        private final boolean isReplica;
        private MultiHandler multiHandler;

        public ClientHandler(Socket socket, boolean isReplica) {
            this.socket = socket;
            this.isReplica = isReplica;
            this.multiHandler = new MultiHandler();
        }

        public void run() {
            try (InputStream in = socket.getInputStream();
                 OutputStream out = socket.getOutputStream();
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"))) {
                while (true) {
                    List<String> command = parseRESP(in);
                    if (command == null || command.isEmpty()) break;
                    String cmd = command.get(0).toUpperCase();

                    if (cmd.equals("PSYNC")) {
                        writer.write("+FULLRESYNC " + master_replid + " " + master_repl_offset + "\r\n");
                        writer.flush();
                        byte[] rdb = new RDB().getEmptyRDB(); // Assume RDB class provides empty RDB
                        out.write(("$" + rdb.length + "\r\n").getBytes());
                        out.write(rdb);
                        out.flush();
                        synchronized (replicaOffsets) {
                            replicaOffsets.put(socket, 0L);
                            replicaOutputs.add(out);
                        }
                    } else if (cmd.equals("REPLCONF") && command.get(1).equalsIgnoreCase("ACK")) {
                        long offset = Long.parseLong(command.get(2));
                        synchronized (replicaOffsets) {
                            replicaOffsets.put(socket, offset);
                        }
                        // No response needed for ACK
                    } else if (cmd.equals("REPLCONF")) {
                        writer.write("+OK\r\n");
                        writer.flush();
                    } else if (cmd.equals("INFO") && command.get(1).equalsIgnoreCase("replication")) {
                        String role = isReplica ? "slave" : "master";
                        String info = "role:" + role + "\r\n" +
                                "master_replid:" + master_replid + "\r\n" +
                                "master_repl_offset:" + master_repl_offset + "\r\n";
                        writer.write("$" + info.length() + "\r\n" + info + "\r\n");
                        writer.flush();
                    } else if (cmd.equals("MULTI")) {
                        multiHandler.init();
                        writer.write("+OK\r\n");
                        writer.flush();
                    } else if (cmd.equals("DISCARD")) {
                        if (!multiHandler.isOn()) {
                            writer.write("-ERR DISCARD without MULTI\r\n");
                        } else {
                            multiHandler.clear();
                            writer.write("+OK\r\n");
                        }
                        writer.flush();
                    } else if (cmd.equals("EXEC")) {
                        if (!multiHandler.isOn()) {
                            writer.write("-ERR EXEC without MULTI\r\n");
                            writer.flush();
                        } else if (multiHandler.isEmpty()) {
                            multiHandler.clear();
                            writer.write("*0\r\n");
                            writer.flush();
                        } else {
                            writer.write("*" + multiHandler.getSize() + "\r\n");
                            while (!multiHandler.isEmpty()) {
                                List<String> queuedCmd = multiHandler.getNext();
                                String msg = execCommand(queuedCmd);
                                if (msg == null) {
                                    writer.write("-ERR unknown command\r\n");
                                } else {
                                    writer.write(msg);
                                }
                            }
                            writer.flush();
                            multiHandler.clear();
                        }
                    } else {
                        if (multiHandler.isOn()) {
                            multiHandler.add(command);
                            writer.write("+QUEUED\r\n");
                            writer.flush();
                        } else {
                            String msg = execCommand(command);
                            if (msg == null) {
                                writer.write("-ERR unknown command\r\n");
                                writer.flush();
                            } else {
                                writer.write(msg);
                                writer.flush();
                            }
                            if (writeCmds.contains(cmd)) {
                                String resp = "*" + command.size() + "\r\n";
                                for (String comp : command) {
                                    resp += "$" + comp.length() + "\r\n" + comp + "\r\n";
                                }
                                master_repl_offset += resp.length(); // Update master offset
                                synchronized (replicaOutputs) {
                                    for (OutputStream replicaOut : replicaOutputs) {
                                        try {
                                            replicaOut.write(resp.getBytes());
                                            replicaOut.flush();
                                        } catch (IOException e) {
                                            System.out.println("Error propagating to replica: " + e.getMessage());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            } finally {
                synchronized (replicaOffsets) {
                    replicaOffsets.remove(socket);
                }
                synchronized (replicaOutputs) {
                    replicaOutputs.remove(socket.getOutputStream());
                }
                try {
                    socket.close();
                } catch (IOException e) {
                    System.out.println("IOException: " + e.getMessage());
                }
            }
        }
    }

    // Placeholder for RDB class
    static class RDB {
        byte[] getEmptyRDB() {
            // Example empty RDB file (base64: UkVESVMwMDEx... as per Redis spec)
            return new byte[]{(byte)0x52, (byte)0x45, (byte)0x44, (byte)0x49, (byte)0x53,
                    (byte)0x30, (byte)0x30, (byte)0x31, (byte)0x31, (byte)0xff};
        }
    }

    // Placeholder for MultiHandler (from your original code)
    static class MultiHandler {
        private boolean isOn = false;
        private List<List<String>> commands = new ArrayList<>();

        void init() {
            isOn = true;
        }

        void clear() {
            isOn = false;
            commands.clear();
        }

        boolean isOn() {
            return isOn;
        }

        boolean isEmpty() {
            return commands.isEmpty();
        }

        int getSize() {
            return commands.size();
        }

        void add(List<String> command) {
            commands.add(command);
        }

        List<String> getNext() {
            return commands.isEmpty() ? null : commands.remove(0);
        }
    }
}
