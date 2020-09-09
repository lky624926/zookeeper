package zookeeper.lky.zookeeper;

import org.apache.zookeeper.*;
import zookeeper.lky.Conf;
import java.util.concurrent.CountDownLatch;

/**
 * 特点:
 * 0.最底层的一种写法
 *
 * 1.会话连接是异步的，需要自己去处理。此处用的CountDownLatch
 *
 * 2.Watch需要重复注册，不然就不能生效.
 *
 * 3.开发的复杂性还是比较高的
 *
 * 4.不支持多节点删除和创建。需要自己去递归。
 */
public class Zookeeper {

    public static void main(String[] args) {

        final CountDownLatch countDown = new CountDownLatch(1);

        Watcher watcher = new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("eventType:" + event.getType());
                    if (event.getType() == Event.EventType.None) {
                        countDown.countDown();
                        System.out.println("none");
                    } else if (event.getType() == Event.EventType.NodeCreated) {
                        System.out.println("listen:节点创建");
                    } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        System.out.println("listen:子节点修改");
                    }else if (event.getType() == Event.EventType.NodeDataChanged){
                        System.out.println("listen:数据修改");
                    }
                }
            }
        };
        try {
            ZooKeeper zookeeper = new ZooKeeper(Conf.zkAddress, 5000,watcher );
            //连接是异步的,必须等连接完成才能进行下面的操作,所以这里要卡一下
            countDown.await();
            //注册监听,每次都要重新注册，否则监听不到
            zookeeper.exists("/aaa", watcher);

            // 创建节点
            String result = zookeeper.create("/aaa", "1234".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(result);

            Thread.sleep(10);

            // 获取节点
            byte[] bs = zookeeper.getData("/aaa", true, null);
            result = new String(bs);
            System.out.println("创建节点后的数据是:" + result);

            // 修改节点
            zookeeper.setData("/aaa", "I love you".getBytes(), -1);

            Thread.sleep(10);

            bs = zookeeper.getData("/aaa", true, null);
            result = new String(bs);
            System.out.println("修改节点后的数据是:" + result);

            // 删除节点
            zookeeper.delete("/aaa", -1);
            System.out.println("节点删除成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
