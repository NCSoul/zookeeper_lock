import org.apache.zookeeper.*;

import java.util.Collections;
import java.util.List;

public class DistributedClientLock {

    //会话超时
    private static final int SESSION_TIMEOUT=5000;
    //zookeeper集群地址
    private String hosts="localhost:2181";
    private String groupNode="locks";
    private String subNode="sub";
    private boolean haveLock=false;

    private ZooKeeper zk;
    private volatile String thisPath;

    private void connectionZookeeper() throws Exception{

        zk=new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

                //判断事件类型，只处理子节点变化
                if (event.getType()== Event.EventType.NodeChildrenChanged && event.getPath().equals("/"+groupNode)){
                    //获取子节点，并对父节点进行监听
                    try {
                        List<String> childNodes=zk.getChildren("/"+groupNode,true);
                        String thisNode=thisPath.substring(("/"+groupNode+"/").length());
                        //判断自己是否是最小的id
                        Collections.sort(childNodes);
                        if (childNodes.indexOf(thisNode)==0){
                            //访问共享资源
                            doSomething();
                            //重新注册一把锁
                            thisPath=zk.create("/"+groupNode+"/"+subNode,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);

                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        });

        //程序一进来就先注册一把锁
        thisPath=zk.create("/"+groupNode+"/"+subNode,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);

        //获取锁父目录下所有自节点，并且对父节点进行监听
        List<String> childrenNodes=zk.getChildren("/"+groupNode,true);

        //如果争抢的资源的程序只有自己，那么就直接去访问共享资源
        if(childrenNodes.size()==1){
            doSomething();
            thisPath=zk.create("/"+groupNode+"/"+subNode,null, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        }

    }

    private void doSomething() throws Exception {
        System.out.println("get lock"+thisPath);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            System.out.println("finished"+thisPath);
            zk.delete(this.thisPath,-1);
        }
    }


    public static void main(String[] args) throws Exception {
        DistributedClientLock distributedClientLock=new DistributedClientLock();
        distributedClientLock.connectionZookeeper();
        Thread.sleep(Integer.MAX_VALUE);
    }

}
