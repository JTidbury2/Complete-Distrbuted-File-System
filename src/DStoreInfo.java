import java.util.ArrayList;

public class DStoreInfo {
private final Object storeLock = new Object();
private Object removeLock = new Object();
boolean storeFlag = true;
String storeMessage = null;

private ArrayList<String> fileList = new ArrayList<>();

public void storeControllerMessage(){
  synchronized(storeLock) {
    System.out.println("Store message wait");
    try {
      storeLock.wait();
      System.out.println("Store message wait done");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

public void storeControllerMessageGo(String message){
  synchronized(storeLock) {
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
        fileList.add(file);
    }
}
