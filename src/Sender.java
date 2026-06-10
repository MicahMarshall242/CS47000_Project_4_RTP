// NOTE:
// The starter code uses a gson encoder for JSON serialization and deserialization. 
// You may replace this with another JSON library of your choice, such as: org.json, Jackson
// Ensure that your chosen library correctly encodes and decodes messages while maintaining 
// the expected structure required by the simulator.

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Sender {
    private static final int DATA_SIZE = 1375;
    private final String host;
    private final int port;
    private final DatagramChannel channel;
    private final Selector selector;
    private InetSocketAddress remoteAddress = null;  
    private boolean waiting = false;
    private final Gson gson = new Gson();

    private final BlockingQueue<String> inputQueue = new LinkedBlockingQueue<>();

    private String pendingData = null;
    private boolean eof = false;

    public Sender(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        channel = DatagramChannel.open();
        channel.bind(new InetSocketAddress(0));
        channel.configureBlocking(false);
        selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);
        InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
        //log("Sender starting up using ephemeral port " + local.getPort());
        log("Sender starting up using port " + port);
    }

    private void log(String message) {
        System.err.println(message);
        System.err.flush();
    }


    private void send(JsonObject message) throws IOException {
        String json = gson.toJson(message);
        log("Sending message '" + json + "'");
        ByteBuffer buffer = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
        channel.send(buffer, new InetSocketAddress(host, port));
    }


    private JsonObject receive() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(65535);
        SocketAddress addr = channel.receive(buffer);
        if (addr == null) {
            return null;
        }
        if (remoteAddress == null) {
            if (addr instanceof InetSocketAddress)
                remoteAddress = (InetSocketAddress) addr;
            else
                remoteAddress = new InetSocketAddress(addr.toString(), 0);
        }
        if (!addr.equals(remoteAddress)) {
            log("Error: Received response from unexpected remote; ignoring");
            return null;
        }
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String jsonStr = new String(bytes, StandardCharsets.UTF_8);
        log("Received message " + jsonStr);
        return gson.fromJson(jsonStr, JsonObject.class);
    }


    private void startInputThread() {
        new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
                char[] charBuffer = new char[DATA_SIZE];
                while (true) {
                    int count = reader.read(charBuffer, 0, DATA_SIZE);
                    if (count == -1) {
                        inputQueue.put("EOF");
                        break;
                    } else if (count > 0) {
                        inputQueue.put(new String(charBuffer, 0, count));
                    }
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
    }

    public void run() throws IOException {
        startInputThread();
        int seq = 0;
        while (true) {
            int readyChannels = selector.select(100);
            if (readyChannels > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isReadable()) {
                        JsonObject data = receive();
                        if (data != null) {
                            waiting = false;  
                        }
                    }
                    iter.remove();
                }
            }

            if (pendingData == null) {
                String input = inputQueue.poll();
                if (input != null) {
                    if (input.equals("EOF")) {
                        eof = true;
                    } else {
                        pendingData = input;
                    }
                }
            }

            if (!waiting && pendingData != null) {
                JsonObject msg = new JsonObject();
                msg.addProperty("type", "msg");
                msg.addProperty("data", pendingData);
                msg.addProperty("seq", seq);
                send(msg);
                waiting = true;
                seq++;
                pendingData = null;
            }

            if (eof && !waiting && pendingData == null) {
                log("All done!");
                System.exit(0);
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java Sender <host> <port>");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        try {
            Sender sender = new Sender(host, port);
            sender.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
