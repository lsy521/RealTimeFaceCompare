import com.hzgc.ftpserver.address.MQSwitchImpl;
import com.hzgc.ftpserver.util.ZookeeperClient;

import java.util.ArrayList;
import java.util.List;

public class ZKClientTest {
    public static void main(String[] args) {
        ZookeeperClient zk = MQSwitchImpl.getZookeeperClient();

        zk.create();

        List<String> ipcIdList = new ArrayList<>();
        ipcIdList.add("aaaa");
        ipcIdList.add("bbbb");
        ipcIdList.add("cccc");
        MQSwitchImpl mqSwitch = new MQSwitchImpl();
        mqSwitch.openMQReception(ipcIdList);

        List<String> aa = zk.getData();
        System.out.println(aa);

        mqSwitch.closeMQReception();
        List<String> bb = zk.getData();
        System.out.println(bb);
    }
}
