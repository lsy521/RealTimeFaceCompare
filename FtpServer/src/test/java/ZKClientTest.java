import com.hzgc.ftpserver.captureSubscription.*;

import java.util.ArrayList;
import java.util.List;

public class ZKClientTest {
    public static void main(String[] args) {

        //MQSwitchInit.create();

        /*MQSwitchStart mqSwitchStart = new MQSwitchStart();
        mqSwitchStart.start();*/
       // MQSwitchInit init = new MQSwitchInit();
        MQSwitchImpl mqSwitch = new MQSwitchImpl();

        /*String userId1 = "user44";
        long time1 = System.currentTimeMillis();
        List<String> user1 = new ArrayList<>();
        user1.add("aa");
        user1.add("bb");
        user1.add("cc");
        mqSwitch.openMQReception(userId1,time1,user1);*/

        /*String userId2 = "user2";
        long time2 = System.currentTimeMillis();//2018-01-19 11:37:36
        List<String> user2 = new ArrayList<>();
        user2.add("xiao");
        user2.add("qi");
        mqSwitch.openMQReception(userId2,time2,user2);*/

        /*List<String> user3 = new ArrayList<>();
        user3.add("55");
        user3.add("xiaoqi");
        mqSwitch.openShow(user3);*/


        mqSwitch.closeMQReception("user44");
    }
}
