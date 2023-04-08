import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ControllerThread implements Runnable {

    Socket controller;
    DStoreInfo info;
    PrintWriter out = null;
    BufferedReader in = null;

    public ControllerThread(Socket controller, DStoreInfo infos) {
        this.controller = controller;
        info = infos;
    }

    @Override
    public void run() {
        try {
            out = new PrintWriter(controller.getOutputStream(), true);
            in = new BufferedReader(
                new InputStreamReader(controller.getInputStream()));
            startThreadWaiters();

            String line;
            while ((line = in.readLine()) != null) {
                System.out.println("Controller thread " + line);
                handleCommand(line);
            }
            controller.close();
            System.out.println("ClientThread connection closed");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void handleCommand(String line) {
    }

    private void startThreadWaiters() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("STORE_ACK watcher started");
                info.storeControllerMessage();
                out.println("STORE_ACK " + info.storeMessage);
                System.out.println("STORE_ACK " + info.storeMessage);
            }
        }).start();
    }
}
