package zookeeper.lky.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import zookeeper.lky.Conf;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁实现
 */
public class Lock {

//    public static void main(String[] args) {
//
//        CuratorFramework cur = CuratorFrameworkFactory.builder()
//                .connectString(Conf.zkAddress)
//                .connectionTimeoutMs(5000)
//                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
//                .build();
//        cur.start();//连接
//
//        InterProcessMutex interProcessMutex = new InterProcessMutex(cur, "/abc");
//        boolean acquire = false;
//        try {
//            acquire = interProcessMutex.acquire(5000L, TimeUnit.SECONDS);
//            //锁获取成功
//            if (acquire) {
//                System.out.println("成功获取到锁");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            if (acquire) {
//                try {
//                    interProcessMutex.release();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {
        DistributedLock joinGroup = new DistributedLock();
        joinGroup.connect(Conf.zkAddress);

        //zookeeper的根节点；运行本程序前，需要提前生成
        String groupName = "zkRoot";
        String memberName = "_locknode_";
        String path = "/" + groupName + "/" + memberName;

        String myName = joinGroup.join(path);
        if (!joinGroup.checkState(path, myName)) {
            joinGroup.listenNode(path, myName);
        }

        Thread.sleep(Integer.MAX_VALUE);

        joinGroup.close();


    }


    public static class ConnectionWatcher implements Watcher {

        private static final int SESSION_TIMEOUT = 999999;

        protected ZooKeeper zk;
        private CountDownLatch connectedSignal = new CountDownLatch(1);

        public void connect(String hosts) throws IOException, InterruptedException {
            zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
            connectedSignal.await();
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        }

        public void close() throws InterruptedException {
            zk.close();
        }
    }


    /**
     * 本类简单实现了分布式锁的机制
     * <p>
     * 采用单机zookeeper服务器测试；运行多次本机程序，相当于多个客户端用户
     * 全部启动完成后，第一个客户端的会话需要手动中断，相当于触发客户端宕机现象
     * <p>
     * <p>
     * 本类实现的分布式锁避免羊群效应（Herd Effect），具体可详见代码
     *
     * @author Liu Dengtao
     * <p>
     * 2014-2-28
     */
    public static class DistributedLock extends ConnectionWatcher {

        public String join(String groupPath)
                throws KeeperException, InterruptedException {

            String path = groupPath + "/lock-" + zk.getSessionId() + "-";

            //建立一个顺序临时节点
            String[] split = groupPath.split("/");
            String path1 = "";
            for (String childPath:split){
                if (!"".equals(childPath)){
                    path1 = path1+"/"+childPath;
                    try {
                        zk.create(path1, null/* data */,
                                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            String createdPath = zk.create(path, null/* data */,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            return createdPath;
        }

        /**
         * 检查本客户端是否得到了分布式锁
         * @param groupPath
         * @param myName
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        public boolean checkState(String groupPath,String myName) throws KeeperException, InterruptedException{

            List<String> childList =  zk.getChildren(groupPath, false);

            String[] myStr = myName.split("-");
            long myId = Long.parseLong(myStr[2]);

            boolean minId = true;
            for (String childName : childList) {
                String[] str = childName.split("-");
                long id = Long.parseLong(str[2]);
                if (id < myId) {
                    minId = false;
                    break;
                }
            }

            if (minId) {
                System.out.println(new Date() + "我得到了分布锁，哈哈！ myId:" + myId);

                return true;
            }else {
                System.out.println(new Date() + "继续努力吧，  myId:" + myId);

                return false;
            }
        }

        /**
         * 若本客户端没有得到分布式锁，则进行监听本节点前面的节点（避免羊群效应）
         * @param groupPath
         * @param myName
         * @throws KeeperException
         * @throws InterruptedException
         */
        public void listenNode(final String groupPath, final String myName) throws KeeperException, InterruptedException{

            List<String> childList =  zk.getChildren(groupPath, false);

            String[] myStr = myName.split("-");
            long myId = Long.parseLong(myStr[2]);

            List<Long> idList = new ArrayList<Long>();
            Map<Long, String> sessionMap = new HashMap<Long, String>();

            for (String childName : childList) {
                String[] str = childName.split("-");
                long id = Long.parseLong(str[2]);
                idList.add(id);
                sessionMap.put(id, str[1]+"-"+str[2]);
            }

            Collections.sort(idList);

            int i = idList.indexOf(myId);
            if (i <=0) {
                throw new IllegalArgumentException("数据错误！");
            }

            //得到前面的一个节点
            long headId = idList.get(i-1);

            String headPath = groupPath + "/lock-" + sessionMap.get(headId);
            System.out.println("添加监听：" + headPath);

            Stat stat = zk.exists(headPath, new Watcher(){

                @Override
                public void process(WatchedEvent event) {
                    System.out.println("已经触发了" + event.getType() + "事件！");

                    try {
                        while(true){
                            if (checkState(groupPath,myName)) {
                                Thread.sleep(3000);
                                System.out.println(new Date() + " 系统关闭！");
                                System.exit(0);
                            }

                            Thread.sleep(3000);

                        }
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            });

            System.out.println(stat);

        }
    }
}
