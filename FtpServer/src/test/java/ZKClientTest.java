import com.hzgc.ftpserver.address.MQSwitchImpl;
import com.hzgc.ftpserver.util.ZookeeperClient;

import java.util.ArrayList;
import java.util.List;

public class ZKClientTest {
    public static void main(String[] args) {
        ZookeeperClient zk = MQSwitchImpl.getZookeeperClient();

        //zk.create();

        MQSwitchImpl mqSwitch = new MQSwitchImpl();
        List<String> user1 = new ArrayList<>();
        user1.add("aaaa");
        user1.add("bbbb");
        user1.add("cccc");
        mqSwitch.openMQReception("user1",user1);
        List<String> data1 = zk.getData();
        System.out.println("user1 open MQ Reception ---> user1 open, user2 close ：" + data1);

        List<String> user2 = new ArrayList<>();
        user2.add("aaaa");
        user2.add("QWER");
        user2.add("777");
        mqSwitch.openMQReception("user2",user2);
        List<String> data2 = zk.getData();
        System.out.println("user2 open MQ Reception ---> user1 open, user2 open ：" + data2);

        mqSwitch.closeMQReception("user1");
        List<String> data3 = zk.getData();
        System.out.println("user1 close MQ Reception ---> user1 close, user2 open ：" + data3);

    }
}
