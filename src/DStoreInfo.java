import java.util.ArrayList;
import java.util.HashMap;

public class DStoreInfo {

    private final int port;
    private final int cport;
    private final int timeOut;

    public DStoreInfo(int port, int cport, int timeOut) {
        this.port = port;
        this.cport = cport;
        this.timeOut = timeOut;
    }


    private final Object storeLock = new Object();
    private Object removeLock = new Object();
    boolean storeFlag = true;
    String storeMessage = null;

    private ArrayList<String> fileList = new ArrayList<>();

    Object rebalanceLock = new Object();

    private HashMap<String, Integer> fileSize = new HashMap<>();

    public void storeControllerMessage() {
        synchronized (storeLock) {
            System.out.println("Store message wait");
            try {
                storeLock.wait();
                System.out.println("Store message wait done");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public int getTimeOut() {
        return timeOut;
    }

    public int getFileSize(String fileName) {
        return fileSize.get(fileName);
    }

    public void addFileSize(String fileName, int size) {
        fileSize.put(fileName, size);
    }

    public void storeControllerMessageGo(String message) {
        synchronized (storeLock) {
            storeMessage = message;
            storeLock.notifyAll();
        }
    }

    public void removeFile(String s) {
        fileList.remove(s);
    }

    public boolean checkFileExist(String fileName) {
        return fileList.contains(fileName);
    }

    public void addFile(String file) {
        if (fileList.contains(file)) {
            return;
        }
        fileList.add(file);
    }


    public ArrayList<String> getFileList() {
        return fileList;
    }

    public String getFiles() {
        String files = "";
        for (String s : fileList) {
            files += s + " ";
        }
        files.trim();
        return files;
    }

    public void rebalanceWait() {
        synchronized (rebalanceLock) {
            try {
                rebalanceLock.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void rebalanceNotify() {
        synchronized (rebalanceLock) {
            rebalanceLock.notifyAll();
        }
    }
}
