package arrow.thea.sockets;

import com.google.gson.JsonElement;
import io.socket.IOAcknowledge;
import io.socket.IOCallback;
import io.socket.SocketIO;
import io.socket.SocketIOException;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.net.MalformedURLException;

/**
 * Created by tpraison on 5/14/16.
 */
public class SocketIOReceiver extends Receiver<String> {

    final String url;

    public SocketIOReceiver(String url) {
        super(StorageLevel.MEMORY_AND_DISK_SER());
        this.url = url;
    }

    @Override
    public StorageLevel storageLevel() {
        return StorageLevel.MEMORY_ONLY();
    }

    @Override
    public void onStart() {
        try {
            final SocketIO socket = new SocketIO(url);
            socket.connect(new IOCallback() {
            public void onMessage(JsonElement json, IOAcknowledge ack) {}

            public void onMessage(String data, IOAcknowledge ack) {}

            public void onError(SocketIOException socketIOException) {
                socketIOException.printStackTrace();
            }

            public void onDisconnect() {}

            public void onConnect() {
                socket.emit("subscribe", "en.wikipedia.org");
                System.out.println("Connection established");
            }

            public void on(String event, IOAcknowledge ack, Object... args) {
                System.out.println("Server triggered event obj '" + event + "'");
            }

            public void on(String event, IOAcknowledge ioAcknowledge, JsonElement... jsonElements) {
                System.out.println("Server triggered event json '" + event + "'" + jsonElements[0].toString());
                socket.disconnect();
                store(jsonElements[0].toString());
            }
        });}
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onStop() {
    }
}
