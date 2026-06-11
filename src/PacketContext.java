import com.google.gson.JsonObject;

public class PacketContext {
    public final JsonObject packet;
    public final long sendTime;

    public PacketContext(JsonObject packet, long sendTime) {
        this.packet = packet;
        this.sendTime = sendTime;
    }
}
