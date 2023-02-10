package cn.edu.tsinghua.iginx.session.DWF;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.DWF_API.DWF;

public class DWF_c {
    public static void main(String[] args) throws SessionException, ExecutionException {
        DWF dwf=new DWF("*","*","*");
        dwf.addDataSource("127.0.0.1",6668, "iotdb11","root","root",false,false);
        dwf.descAllColumnDataSource("127.0.0.1",6667,"root","root");
        dwf.descAllTableDataSource("127.0.0.1",6667,"root","root");

        dwf.descAllDBDataSource("127.0.0.1",6667,"root","root");
        System.out.println(String.valueOf(dwf.removeDataSource(3)));
        System.out.println("success");
    }
}

