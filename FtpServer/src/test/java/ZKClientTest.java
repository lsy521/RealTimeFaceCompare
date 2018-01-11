import com.hzgc.ftpserver.util.ZookeeperClient;

import java.util.ArrayList;
import java.util.List;

public class ZKClientTest {
    public static void main(String[] args) {
        ZookeeperClient zk = new ZookeeperClient(10000,
                "172.18.18.103:2181,172.18.18.104:2181,172.18.18.105:2181",
                "/777",false);

        //zk.create();

        List<String> ipcIdList = new ArrayList<>();
        ipcIdList.add("aaaa");
        ipcIdList.add("bbbb");
        ipcIdList.add("333");
        zk.setData(null);

        List<String> aa = zk.getData();
        System.out.println(aa);
    }
}
