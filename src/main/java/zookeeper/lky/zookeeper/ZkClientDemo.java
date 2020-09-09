package zookeeper.lky.zookeeper;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import zookeeper.lky.Conf;

import java.util.List;

/**
 * 比直接使用zookeeper好一点,比较推荐
 */
public class ZkClientDemo {

    public static void main(String[] args) {
        try {
            ZkClient zk = new ZkClient(Conf.zkAddress);

            // 注册【数据】事件
            zk.subscribeDataChanges("/top", new IZkDataListener() {

                @Override
                public void handleDataDeleted(String arg0) throws Exception {
                    System.out.println("数据删除:" + arg0);

                }

                @Override
                public void handleDataChange(String arg0, Object arg1) throws Exception {
                    System.out.println("数据修改:" + arg0 + "------" + arg1);

                }
            });

            //注册子节点事件
            zk.subscribeChildChanges("/top", new IZkChildListener() {

                @Override
                public void handleChildChange(String arg0, List<String> arg1) throws Exception {
                    System.out.println("子节点发生变化：" + arg0);
                    arg1.forEach(f -> {
                        System.out.println("content：" + f);
                    });
                }
            });

            //这块儿没用内部会发生的一些变化,外部感知不到
            zk.subscribeStateChanges( new IZkStateListener() {
                @Override
                public void handleStateChanged(Watcher.Event.KeeperState state) throws Exception {
                    System.out.println("状态变化"+state);
                }

                @Override
                public void handleNewSession() throws Exception {
                    System.out.println("新session");
                }

                @Override
                public void handleSessionEstablishmentError(Throwable error) throws Exception {
                    System.out.println("session错误"+error);
                }
            });

            List<String> list = zk.getChildren("/");
            list.forEach(e -> {
                System.out.println(e);
            });
            String top = zk.create("/top", "you", CreateMode.PERSISTENT);
            System.out.println("创建节点/top成功:" + top);
            String res = zk.create("/top/zhuzhu", "I love you", CreateMode.PERSISTENT);
            System.out.println("创建节点/top/zhuzhu成功:" + res);

            zk.writeData("/top/zhuzhu", "forerver");
            System.out.println("修改节点/top/zhuzhu数据成功");

            res = zk.readData("/top/zhuzhu");
            System.out.println("节点数据:" + res);

            Thread.sleep(1000);

            zk.delete("/top/zhuzhu");
            System.out.println("删除节点/top/zhuzhu成功");

            Thread.sleep(1000);

            System.out.println("------------------------------------------------");

            zk.delete("/top");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
