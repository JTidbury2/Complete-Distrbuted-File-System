import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.HashMap;

public class ControllerInfo {

  private int cport = 0;
  private int repFactor = 0;
  private int timeOut = 0;
  private int rebalanceTime = 0;
  private ArrayList<Integer> dstoreList = new ArrayList<Integer>();
  private ArrayList<String> fileList = new ArrayList<String>();
  private HashMap <String, ArrayList<Integer>> fileDstoreMap = new HashMap<String, ArrayList<Integer>>();
  private HashMap <String, String> fileIndex = new HashMap<String, String>();
  private ArrayList <Integer> storeAcks = new ArrayList<Integer>();


  private Object storeLock = new Object();

  public int getCport() {
    return cport;
  }

  public int getRepFactor() {
    return repFactor;
  }

  public void setRepFactor(int repFactor) {
    this.repFactor = repFactor;
  }

  public int getTimeOut() {
    return timeOut;
  }

  public void setTimeOut(int timeOut) {
    this.timeOut = timeOut;
  }

  public int getRebalanceTime() {
    return rebalanceTime;
  }

  public void setRebalanceTime(int rebalanceTime) {
    this.rebalanceTime = rebalanceTime;
  }

  public void setFileIndex(String file, String index) {
    fileIndex.put(file, index);
  }

  public String getFileIndex(String file) {
    return fileIndex.get(file);
  }

  public Integer[] getStoreDStores(){
    Integer[] dstores = new Integer[repFactor];
    dstores = dstoreList.subList(0,repFactor).toArray(dstores);
    return dstores;
  }

  public ArrayList<Integer> getDstoreList() {
    return dstoreList;
  }

  public void setDstoreList(ArrayList<Integer> dstoreList) {
    this.dstoreList = dstoreList;
  }

  public void addDstore(int dstore) {
    dstoreList.add(dstore);
  }

  public void setCport(int cport) {
    this.cport = cport;
  }

  public String list() throws NotEnoughDstoresException {
    if (dstoreList.size() < repFactor) {
      throw new NotEnoughDstoresException();
    }
    String message = "";
    for (String file : fileList) {
      message += file + " ";

    }
    message.trim();
    message = "LIST " + message;
    return message;
  }

  public String storeTo (String name) throws NotEnoughDstoresException, FileAlreadyExistsException {
    if (dstoreList.size() < repFactor) {
      throw new NotEnoughDstoresException();
    }
    if (fileList.contains(name)) {
      throw new FileAlreadyExistsException(name);
    }
    Integer[] dstores = getStoreDStores();
    String message = "STORE_TO ";
    for (Integer num : dstores) {
      message += num + " ";
    }
    message.trim();
    return message;

  }

  public void storeAck (int dstore, String file) {
    storeAcks.add(dstore);
    if (storeAcks.size() == dstoreList.size()) {
      storeAcks.clear();
      setFileIndex(file, "SC");
      fileList.add(file);
      storeComplete();
    }
  }

  public void storeComplete (){
    synchronized (storeLock) {
      storeLock.notify();
    }
  }

  public void storeWait() {
    synchronized (storeLock) {
      try {
        storeLock.wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
