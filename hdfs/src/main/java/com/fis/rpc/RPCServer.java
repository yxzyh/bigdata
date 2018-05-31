package com.fis.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * @author Geoge
 * @date 2018/5/23 0023 下午 2:37
 */
public class RPCServer implements Bizable{

    public String sayHi(String name){
        return "Hi..." + name;
    }

    public static void main(String[] args) throws IOException {

        Configuration conf  = new Configuration();

       RPC.Server server =  new RPC.Builder(conf)
               .setProtocol(Bizable.class)
               .setInstance(new RPCServer())
               .setBindAddress("192.168.187.1")
               .setPort(9527)
               .build();

       server.start();

    }
}
