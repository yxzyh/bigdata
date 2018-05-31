package com.fis.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Geoge
 * @date 2018/5/23 0023 下午 2:50
 */
public class RPCClient {

    public static void main(String[] args) throws IOException {
       Bizable proxy =  RPC.getProxy(Bizable.class,10010,new InetSocketAddress("192.168.187.1",9527),new Configuration());
       String result = proxy.sayHi("Tomcat Dom");
       System.out.println(result);
       RPC.stopProxy(proxy);

    }

}
