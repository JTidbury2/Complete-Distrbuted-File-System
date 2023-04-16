import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class Controller {

    static ControllerInfo info = new ControllerInfo();
    static ServerSocket ss;
    static int cport = 0;
    static int repFactor = 0;
    static int timeOut = 0;
    static int rebalanceTime = 0;

    public static void main(String[] args) {
        cport = Integer.parseInt(args[0]);
        repFactor = Integer.parseInt(args[1]);
        timeOut = Integer.parseInt(args[2]);
        rebalanceTime = Integer.parseInt(args[3]);
        info.setCport(cport);
        info.setRepFactor(repFactor);
        info.setTimeOut(timeOut);
        info.setRebalanceTime(rebalanceTime);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                System.out.println("Rebalance timer started");
                info.rebalance(new String[0],0);
                info.rebalanceStart();

            }
        },0, info.getRebalanceTime());
        setUpCPort();
    }

    public static void setUpDstoreThread(Socket client, int port) {
        System.out.println("DStoreThread" + port + " started");
        new Thread(new DStoreThread(client, port, info), "DStore Thread " + port).start();
    }

    public static void setUpClientThread(Socket client, String firstCommand) {
        System.out.println("ClientThread " + client.getPort() + " started");
        new Thread(new ClientThread(client, firstCommand, info),
            "Client Thread " + client.getPort()).start();
    }

    private static void setUpCPort() {
        try {
            ss = new ServerSocket(cport);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                final Socket client = ss.accept();
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            boolean closeFlag = true;
                            BufferedReader in = new BufferedReader(
                                new InputStreamReader(client.getInputStream()));
                            String line;
                            System.out.println("Connection to unknown accepted");
                            while ((line = in.readLine()) != null) {
                                if (line.startsWith("JOIN")) {
                                    String[] split = line.split(" ");
                                    String dstore = split[1];
                                    info.addDstore(Integer.parseInt(dstore));
                                    setUpDstoreThread(client, Integer.parseInt(dstore));
                                    closeFlag = false;
                                    break;
                                } else if ((line.startsWith("LIST") || line.startsWith("STORE")
                                    || line.startsWith(
                                    "REMOVE") || line.startsWith("LOAD"))) {
                                    setUpClientThread(client, line);
                                    closeFlag = false;
                                    break;
                                }
                            }
                            if (closeFlag) {
                                client.close();
                                System.out.println("Connection to unknown closed. Port number: "
                                    + client.getPort());
                            }


                        } catch (IOException e) {
                        }
                    }
                }).start();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }


    }

}
