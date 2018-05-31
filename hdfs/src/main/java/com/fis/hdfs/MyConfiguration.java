package com.fis.hdfs;

/**
 * @author Geoge
 * @date 2018/4/19 0019 下午 4:38
 */
public class MyConfiguration {
    private String data = null;
    public MyConfiguration(){
        this(true);
    }

    public MyConfiguration(boolean loadDefaults){
        if(loadDefaults){
            data = "A";

        }else{
            data = "B";
        }
        System.out.println(data);
    }

    public static void main(String[] args) {
        MyConfiguration myConfiguration = new MyConfiguration(false);

    }
}
