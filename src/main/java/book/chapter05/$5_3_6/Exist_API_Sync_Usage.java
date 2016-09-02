package book.chapter05.$5_3_6;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

// ZooKeeper API 删除节点，使用同步(sync)接口。
public class Exist_API_Sync_Usage implements Watcher {

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zk;
    public static void main(String[] args) throws Exception {

    	String path = "/zk-book";
    	zk = new ZooKeeper("127.0.0.1:2181", 
				5000, //
				new Exist_API_Sync_Usage());
    	connectedSemaphore.await();

    	zk.exists( path, true );
    	
    	zk.create( path, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
    	
    	zk.setData( path, "123".getBytes(), -1 );
    	
    	zk.create( path+"/c1", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT );
    	
    	zk.delete( path+"/c1", -1 );
    	
    	zk.delete( path, -1 );
    	
        Thread.sleep( Integer.MAX_VALUE );
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (KeeperState.SyncConnected == event.getState()) {
                if (EventType.None == event.getType() && null == event.getPath()) {
                    connectedSemaphore.countDown();
                } else if (EventType.NodeCreated == event.getType()) {
                    Stat s0 = zk.exists( event.getPath(), true );
                    System.out.println("Node(" + event.getPath() + ")Created "+s0.getVersion());
                } else if (EventType.NodeDeleted == event.getType()) {
                    zk.exists( event.getPath(), true );
                    System.out.println("Node(" + event.getPath() + ")Deleted ");
                } else if (EventType.NodeDataChanged == event.getType()) {
                	Stat s0 = zk.exists( event.getPath(), true );
                    System.out.println("Node(" + event.getPath() + ")DataChanged "+s0.getVersion());
                }
            }
        } catch (Exception e) {}
    }
}