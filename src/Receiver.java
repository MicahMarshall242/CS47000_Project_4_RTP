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
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Receiver {
    private DatagramChannel channel;
    private Selector selector;
    private InetSocketAddress remoteAddress = null;  
    private Gson gson = new Gson();

    public Receiver() throws IOException {
        channel = DatagramChannel.open();
        channel.bind(new InetSocketAddress(0));
        channel.configureBlocking(false);
        selector = Selector.open();
        channel.register(selector, SelectionKey.OP_READ);
        InetSocketAddress local = (InetSocketAddress) channel.getLocalAddress();
        log("Bound to port " + local.getPort());
    }

    private void log(String message) {
        System.err.println(message);
    }


    private void send(JsonObject message) throws IOException {
        String json = gson.toJson(message);
        log("Sent message " + json);
        if (remoteAddress != null) {
            ByteBuffer buffer = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8));
            channel.send(buffer, remoteAddress);
        }
    }


    private JsonObject receive() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(65535);
        SocketAddress addr = channel.receive(buffer);
        if (addr == null)
            return null;
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

    public void run() throws IOException {
        while (true) {
            int readyChannels = selector.select();
            if (readyChannels > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isReadable()) {
                        JsonObject msg = receive();
                        if (msg != null) {
                            if (msg.has("data")) {
                                System.out.print(msg.get("data").getAsString());
                                System.out.flush();
                            }
                            if (msg.has("seq")) {
                                JsonObject ack = new JsonObject();
                                ack.addProperty("type", "ack");
                                ack.addProperty("seq", msg.get("seq").getAsInt());
                                send(ack);
                            }
                        }
                    }
                    iter.remove();
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Receiver receiver = new Receiver();
            receiver.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
