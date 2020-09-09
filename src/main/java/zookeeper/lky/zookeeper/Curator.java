package zookeeper.lky.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import zookeeper.lky.Conf;

/**
 * 比较高端的写法
 */
public class Curator {

    public static void main(String[] args) throws Exception {
        CuratorFramework cur= CuratorFrameworkFactory.builder()
                .connectString(Conf.zkAddress)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();
        cur.start();//连接

        //创建监听
        PathChildrenCache cache=new PathChildrenCache(cur,"/top",true);
        cache.start();
        cache.rebuild();
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework framwork, PathChildrenCacheEvent event) throws Exception {
                System.err.println("节点发生变化:"+event.getType());
            }
        });

        Stat stat=cur.checkExists().forPath("/top/zhuzhu");
        if(stat!=null){
            System.out.println("【/top/zhuzhu】节点存在，直接删除");
            cur.delete().forPath("/top/zhuzhu");
        }

        System.out.println("准备创建【/top/zhuzhu】");
        cur.create().withMode(CreateMode.PERSISTENT)
                .forPath("/top/zhuzhu", "love forever".getBytes());
        System.out.println("节点【/top/zhuzhu】创建成功");

        Thread.sleep(1000);

        byte[] bs=cur.getData().forPath("/top/zhuzhu");
        System.out.println("数据:"+new String(bs));

        Thread.sleep(1000);

        cur.delete().forPath("/top/zhuzhu");

        Thread.sleep(1000);

    }


    /**
     * 三种watcher来做节点的监听
     * pathcache   监视一个路径下子节点的创建、删除、节点数据更新
     * NodeCache   监视一个节点的创建、更新、删除
     * TreeCache   pathcaceh+nodecache 的合体（监视路径下的创建、更新、删除事件），
     * 缓存路径下的所有子节点的数据
     */

    public static void main1(String[] args) throws Exception {
        CuratorFramework curatorFramework=CuratorFrameworkFactory.builder()
                .connectString(Conf.zkAddress)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();
        curatorFramework.start();

        /**
         * 节点变化NodeCache
         */
       /* NodeCache cache=new NodeCache(curatorFramework,"/curator",false);
        cache.start(true);

        cache.getListenable().addListener(()-> System.out.println("节点数据发生变化,变化后的结果" +
                "："+new String(cache.getCurrentData().getData())));

        curatorFramework.setData().forPath("/curator","菲菲".getBytes());*/


        /**
         * PatchChildrenCache
         */

        PathChildrenCache cache=new PathChildrenCache(curatorFramework,"/event",true);
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.rebuild();
        // Normal / BUILD_INITIAL_CACHE /POST_INITIALIZED_EVENT

        cache.getListenable().addListener((curatorFramework1,pathChildrenCacheEvent)->{
            switch (pathChildrenCacheEvent.getType()){
                case CHILD_ADDED:
                    System.out.println("增加子节点");
                    break;
                case CHILD_REMOVED:
                    System.out.println("删除子节点");
                    break;
                case CHILD_UPDATED:
                    System.out.println("更新子节点");
                    break;
                default:break;
            }
        });

        curatorFramework.create().forPath("/event","aaa".getBytes());
        curatorFramework.create().forPath("/event/event1","aaa".getBytes());
        //  curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath("/event","event".getBytes());
        // TimeUnit.SECONDS.sleep(1);
        // System.out.println("1");
//        curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath("/event/event1","1".getBytes());
//        TimeUnit.SECONDS.sleep(1);
//        System.out.println("2");
//
//        curatorFramework.setData().forPath("/event/event1","222".getBytes());
//        TimeUnit.SECONDS.sleep(1);
//        System.out.println("3");

        curatorFramework.delete().forPath("/event/event1");
        curatorFramework.delete().forPath("/event");
        System.out.println("4");

    }
}
