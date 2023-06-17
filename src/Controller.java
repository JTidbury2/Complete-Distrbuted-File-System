import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;

public class Controller {

    static ControllerInfo info;
    static ServerSocket ss;
    static int cport = 0;
    static int repFactor = 0;
    static int timeOut = 0;
    static int rebalanceTime = 0;


    public static void main(String[] args) {
        // cport is controller port number
        cport = Integer.parseInt(args[0]);
        repFactor = Integer.parseInt(args[1]);
        timeOut = Integer.parseInt(args[2]);
        rebalanceTime = Integer.parseInt(args[3]);
        info = new ControllerInfo(cport, repFactor, timeOut, rebalanceTime);
        setUpCPort();
    }

    /**
     * Sets up a new dstore thread with the given port number and socket
     *
     * @param client The socket used to communicate to the dstore
     * @param port   The port number of the dstore
     */
    public static void setUpDstoreThread(Socket client, int port) {
        System.out.println("DStoreThread" + port + " started");

        new Thread(new DStoreThread(client, port, info), "DStore Thread " + port).start();


    }

    /**
     * Sets up a new client thread with the given socket and first command to run on the thread
     *
     * @param client       The socket used to communicate to the client
     * @param firstCommand The first command to run on the thread
     */
    public static void setUpClientThread(Socket client, String firstCommand) {
        System.out.println("ClientThread " + client.getPort() + " started");
        new Thread(new ClientThread(client, firstCommand, info),
                "Client Thread " + client.getPort()).start();
    }

    /**
     * Sets up the Control port.
     * Listens for connections from the client or the DStore,
     * then initiates appropriate handling threads based on the received messages.
     */
    private static void setUpCPort() {
        // Establish server socket at Control port
        try {
            ss = new ServerSocket(cport);
        } catch (IOException e) {
            // Print the stack trace for debugging if exception occurs
            e.printStackTrace();
        }
        // Continuously listen for incoming connections
        while (true) {
            try {
                // Accept the connection
                final Socket client = ss.accept();
                // Start a new thread for each client
                new Thread(new Runnable() {
                    public void run() {
                        try {
                            // Flag to decide whether to close the client connection
                            boolean closeFlag = true;

                            // Initialize a reader to read the input stream from the client
                            BufferedReader in = new BufferedReader(
                                    new InputStreamReader(client.getInputStream()));
                            String line;

                            // Notify that a connection was accepted
                            System.out.println("Connection to unknown accepted");

                            // Listen for incoming messages
                            while ((line = in.readLine()) != null) {

                                // If JOIN message is received, set up a DStore thread
                                if (line.startsWith("JOIN")) {

                                    String[] split = line.split(" ");
                                    String dstore = split[1];
                                    info.addDstore(Integer.parseInt(dstore));
                                    setUpDstoreThread(client, Integer.parseInt(dstore));
                                    closeFlag = false;
                                    break;

                                    // If LIST, STORE, REMOVE or LOAD messages are received, set up a Client thread
                                } else if ((line.startsWith("LIST") || line.startsWith("STORE")
                                        || line.startsWith("REMOVE") || line.startsWith("LOAD"))) {

                                    setUpClientThread(client, line);
                                    closeFlag = false;
                                    break;
                                }
                            }

                            // If no known commands are received, close the client connection
                            if (closeFlag) {
                                client.close();

                                // Notify that a connection was closed
                                System.out.println("Connection to unknown closed. Port number: "
                                        + client.getPort());
                            }

                        } catch (IOException e) {
                            // Handle the case where the connection is reset
                        }
                    }
                }).start();
            } catch (SocketException e) {
                // Handle the case where the connection is reset
                if (e.getMessage().equals("Connection reset")) {
                    e.printStackTrace();
                }
            } catch (IOException e) {
                // Print the stack trace for debugging if other IOException occurs
                e.printStackTrace();
            }

        }
    }
}