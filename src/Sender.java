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
    private static final int maxWindowSize = 5;
    public  int RTTms = 300;
    private final Map<Integer, PacketContext> packetWindow;
    private int nextSeq = 0;
    private final FILOFixedBuffer<Long> slidingWindow;


    private final BlockingQueue<String> inputQueue = new LinkedBlockingQueue<>();

    private String pendingData = null;
    private boolean eof = false;

    public Sender(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.packetWindow = new HashMap<>(); //             NOTE: not using this yet
        channel = DatagramChannel.open();
        channel.bind(new InetSocketAddress(0));
        channel.configureBlocking(false);
        selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);
        InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
        this.slidingWindow = new FILOFixedBuffer<Long>(maxWindowSize); // make the sliding window same as buffer
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
        while (true) {
            int readyChannels = selector.select(100);
            if (readyChannels > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isReadable()) {
                        JsonObject data = receive();
                        if (data != null) {
                            handleIncomingAck(data);
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
                sendNext();
            }

            if (!packetWindow.isEmpty()) {
                retransmitOutdated();
            }

            if (eof && !waiting && pendingData == null && packetWindow.isEmpty()) {
                log("All done!");
                System.exit(0);
            }

            log("current items in flight: " + packetWindow.size());
        }
    }

    private void handleIncomingAck(JsonObject ack) {
        // data filtering
        if (!ack.has("type")) {
            log("Unknown packet: " + ack); return;
        }
        if (!ack.get("type").getAsString().equals("ack")) {
            log("Non-Ack Received!"); return;
        }
        int seq = ack.get("seq").getAsInt();

        PacketContext ctx = packetWindow.get(seq);  // look up the corresponding sent packet context
        if (ctx == null) {
            log("Questionable or Duplicate ACK Received.");
            return; // we never sent a packet corresponding to this seq#, or we've already received an ack -> discard
        }

        if (ctx.packet.get("seq").getAsInt() == seq) {
            packetWindow.remove(seq); // remove this packet from the window
            long diff = System.currentTimeMillis() - ctx.sendTime;
            slidingWindow.push(diff);
            recomputeRTT();         // estimate rtt
            waiting = false;     // we can now send another packet
        }
    }

    private void sendNext() throws IOException {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "msg");
        msg.addProperty("data", pendingData);
        msg.addProperty("seq", nextSeq);
        packetWindow.put(nextSeq, new PacketContext(msg, System.currentTimeMillis())); // save this message until it's acked

        send(msg); // send it out

        if (packetWindow.size() >= maxWindowSize) {
            waiting = true; // we have no more 'space' to send other packets until the in-flight ones are acked
        }
        nextSeq++; // just increment by 1 for now
        pendingData = null;
    }

    private void retransmitOutdated() throws IOException {
        for (Map.Entry<Integer, PacketContext> entry : packetWindow.entrySet()) {
            PacketContext ctx = entry.getValue();
            int seq = entry.getKey();
            long diff = System.currentTimeMillis() - ctx.sendTime;
            log("seq: " + seq +  " diff: " + diff);

            if (diff > RTTms * 2L) {
                log("Resending packet...");
                send(ctx.packet);
                packetWindow.put(seq, new PacketContext(ctx.packet, System.currentTimeMillis()));  // update the send time
            }
        }
    }

    private void recomputeRTT() {
        int rtt = 0;
        int limit = Math.min(maxWindowSize, slidingWindow.size());
        for (int i = 0; i < limit; i++) {
            rtt += slidingWindow.get(i);
        }
        rtt /= limit;
        log("New RTT: " + rtt);
        this.RTTms = rtt;
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
