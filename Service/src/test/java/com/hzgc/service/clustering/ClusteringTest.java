package com.hzgc.service.clustering;

import com.hzgc.dubbo.clustering.ClusteringAttribute;
import com.hzgc.dubbo.clustering.ClusteringInfo;
import com.hzgc.service.util.HBaseHelper;
import com.hzgc.service.util.HBaseUtil;
import com.hzgc.util.common.ObjectUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 告警聚类信息读写测试类及方法（彭聪）
 */
public class ClusteringTest {
    public static void main(String[] args) {
       /* List<ClusteringAttribute> clusteringAttributeList = new ArrayList<>();
        String rowkey = "2018-06";
        ClusteringAttribute clusteringAttribute = new ClusteringAttribute();
        clusteringAttribute.setClusteringId("1");
        clusteringAttribute.setFirstAppearTime("2018-01-15");
        clusteringAttribute.setFtpUrl("ftp://s105/skdfjllj/fjdslafjl.jpg");
        clusteringAttributeList.add(clusteringAttribute);
        clusteringAttribute.setClusteringId("2");
        clusteringAttribute.setFirstAppearTime("2018-01-15");
        clusteringAttribute.setFtpUrl("ftp://s105/skdfjllj/fjdslafjl.jpg");
        clusteringAttribute.setClusteringId("3");
        clusteringAttribute.setFirstAppearTime("2018-01-15");
        clusteringAttribute.setFtpUrl("ftp://s105/skdfjllj/fjdslafjl.jpg");
        clusteringAttributeList.add(clusteringAttribute);
        clusteringAttribute.setCount(3);
        clusteringAttributeList.add(clusteringAttribute);
        putDataToHBase(rowkey, clusteringAttributeList);
        getDataFromHBase("2018-06");*/

        /*ClusteringSearchServiceImpl clusteringSearchService = new ClusteringSearchServiceImpl();
        ClusteringInfo clusteringInfo = clusteringSearchService.clusteringSearch("2018-02", 0, 50, "");
        System.out.println(clusteringInfo.getTotalClustering());
        List<ClusteringAttribute> clusteringAttributeList = clusteringInfo.getClusteringAttributeList();
        System.out.println(clusteringAttributeList.get(0).getCount());
        System.out.println(clusteringAttributeList.get(0).getClusteringId());*/



       /* List<Integer> blist = new java.util.ArrayList<>();
        blist.add(1);
        blist.add(2);
        blist.add(4);
        blist.add(7);
        blist.add(9);
        blist.add(10);
        blist.add(11);
        blist.add(12);
        blist.add(13);
        List<ClusteringAttribute> clusteringAttributeList2 = new ArrayList<>();
        for (int i = 0; i < clusteringAttributeList1.size(); i++) {
            if (blist.contains(i)){
                clusteringAttributeList2.add(clusteringAttributeList1.get(i));
            }
        }
        List<ClusteringAttribute> clusteringAttributeList2 = new ArrayList<>();
        clusteringAttributeList2.addAll(clusteringAttributeList1);
        putDataToHBase("2018-02-bak", clusteringAttributeList2);*/
        ClusteringSearchServiceImpl clusteringSearchService = new ClusteringSearchServiceImpl();
       /* ClusteringInfo clusteringInfo;
        clusteringInfo = clusteringSearchService.clusteringSearch("1", "2018-02", 0, 50, "");
        List<ClusteringAttribute> clusteringAttributeList;
        clusteringAttributeList = clusteringInfo.getClusteringAttributeList();
        for (ClusteringAttribute clusteringAttributeList1 : clusteringAttributeList) {
            String clusterId = clusteringAttributeList1.getClusteringId();
            System.out.println(clusterId);
            String ipcID = clusteringAttributeList1.getFirstIpcId();
            System.out.println(ipcID);
            System.out.println(clusteringAttributeList1.getCount());
        }*/
        List<Integer> alarmInfos = clusteringSearchService.detailClusteringSearch_v1("1-3-81840d1f-705c-4586-8c10-12b8a89a9668", "2018-02", 0, 1000, "");
        System.out.println(alarmInfos.size());
        for (Integer alarmInfos1 : alarmInfos) {
            System.out.println(alarmInfos1);
        }

        /*idList.add("1-19");
        clusteringSearchService.ignoreClustering(idList, "2018-02", "no");*/

        /*clusteringInfo = clusteringSearchService.clusteringSearch("1", "2018-02", 30, 50, "");
        System.out.println(clusteringInfo.getTotalClustering());
        System.out.println(clusteringInfo.getClusteringAttributeList().get(1).getClusteringId());*/
    }

    private static void putDataToHBase(String rowKey, List<ClusteringAttribute> clusteringAttributeList) {
        Table clusteringInfoTable = HBaseHelper.getTable(ClusteringTable.TABLE_ClUSTERINGINFO);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(ClusteringTable.ClUSTERINGINFO_COLUMNFAMILY, ClusteringTable.ClUSTERINGINFO_COLUMN_YES, ObjectUtil.objectToByte(clusteringAttributeList));
        try {
            clusteringInfoTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.closTable(clusteringInfoTable);
        }
    }

    private static void getDataFromHBase(String rowKey) {
        Table clusteringInfoTable = HBaseHelper.getTable(ClusteringTable.TABLE_ClUSTERINGINFO);
        Get get = new Get(Bytes.toBytes(rowKey));
        List<ClusteringAttribute> clusteringAttributeList;
        ClusteringAttribute clusteringAttribute;
        try {
            Result result = clusteringInfoTable.get(get);
            clusteringAttributeList = (List<ClusteringAttribute>) ObjectUtil.byteToObject(result.getValue(ClusteringTable.ClUSTERINGINFO_COLUMNFAMILY, ClusteringTable.ClUSTERINGINFO_COLUMN_YES));
            clusteringAttribute = clusteringAttributeList.get(0);
            System.out.println(clusteringAttribute.getClusteringId());
            System.out.println(clusteringAttribute.getCount());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
