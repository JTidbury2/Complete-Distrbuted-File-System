public class DStoreInfo {
private final Object storeLock = new Object();
private Object removeLock = new Object();
boolean storeFlag = true;
String storeMessage = null;

public void storeControllerMessage(){
  synchronized(storeLock) {
    try {
      storeLock.wait();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}

public void storeControllerMessageGo(String message){
  synchronized(storeLock) {
    storeMessage = message;
    storeLock.notify();
  }
}}
