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
  private HashMap <String, Index> fileIndex = new HashMap<String, Index>();
  private HashMap <String,ArrayList<Integer>> storeAcks = new HashMap<String, ArrayList<Integer>>();
  private Object storeLock = new Object();

  private HashMap <String, Integer> fileSizeMap = new HashMap<String, Integer>();


  public Integer[] getStoreDStores(){
    Integer[] dstores = new Integer[repFactor];
    dstores = dstoreList.subList(0,repFactor).toArray(dstores);
    return dstores;
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

  public void storeAck (String file) {
    if (storeAcks.containsKey(file)) {
      storeAcks.get(file).add(1);
    } else {
      ArrayList<Integer> acks = new ArrayList<Integer>();
      acks.add(1);
      storeAcks.put(file, acks);
    }
    if (storeAcks.get(file).size() == repFactor) {
      System.out.println("Store complete");
      storeAcks.clear();
      setFileIndex(file, Index.STORE_IN_PROGRESS);
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

  public void setFileIndex(String file, Index index) {
    fileIndex.put(file, index);
  }

  public Index getFileIndex(String file) {
    return fileIndex.get(file);
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

  public int[] getFileDStores(String s,int times)
      throws NotEnoughDstoresException, FileDoesNotExistException, DStoreCantRecieveException {
    if (!fileDstoreMap.containsKey(s)) {
      throw new FileDoesNotExistException(s);
    } else if (dstoreList.size()<repFactor) {
      throw new NotEnoughDstoresException();
    } else if (fileDstoreMap.get(s).size() < times) {
      throw new DStoreCantRecieveException();
    }
    int[] result = new int[2];
    result[0] = fileDstoreMap.get(s).get(times);
    result[1] = fileSizeMap.get(s);
    return result;

  }

  }

