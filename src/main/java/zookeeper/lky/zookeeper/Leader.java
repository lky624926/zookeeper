package zookeeper.lky.zookeeper;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import zookeeper.lky.Conf;

/**
 * zk选举
 */
public class Leader {

    private static ZkClient zkClient = new ZkClient(Conf.zkAddress,500,5000);
    private static String path = "/election";

    public static void main(String[] args) {

        System.out.println("项目启动成功！");
        //1,项目启动的时候  在zk创建临时节点
        createEphemeral();
        //2，谁能够创建成功谁就是主服务器
        //3,使用服务监听节点是否被删除，如果被删。 重新开始创建节点
        zkClient.subscribeDataChanges(path, new IZkDataListener() {
            //返回节点如果被删除后 返回通知
            @Override
            public void handleDataDeleted(String arg0) throws Exception {
                //重新创建（选举）
                System.out.println("开始重新选举策略");
                System.out.println(System.currentTimeMillis());
                createEphemeral();
            }

            @Override
            public void handleDataChange(String arg0, Object arg1) throws Exception {
                // TODO Auto-generated method stub

            }
        });

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void createEphemeral() {
        try {
            zkClient.createEphemeral(path);
            System.out.println("选举成功！");
        } catch (Exception e) {
            System.out.println("该节点已经存在");
        }
    }

}
