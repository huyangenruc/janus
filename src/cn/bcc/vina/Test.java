package cn.bcc.vina;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.*;
import javax.servlet.http.*;

import org.apache.hadoop.http.HtmlQuoting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;

import cn.bcc.meta.HadoopConf;
import cn.bcc.util.FileOperation;
import cn.bcc.util.HadoopFile;

public class Test {
    private static void hint() {
        System.out.println("Continue to run a vina hadoop demo? [yes]/[no]");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            String command = reader.readLine();


            if ("yes".equals(command.trim())) {
                System.out.println("please input vina jobid,eg:20130909 [yes]/[no]");
                BufferedReader newReader = new BufferedReader(new InputStreamReader(System.in));
                command = newReader.readLine();
                System.out.println(command.trim());
            } else {
                System.out.println("okay,exit now");
                System.exit(0);
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    

    public static void main(String args[]) throws Exception {

        VinaHadoop job = new VinaHadoop();
        String vinaJobID = "20130927";
        ArrayList<String> test = new ArrayList<String>();
        test.add("/bcc_free");
        int topK = 100;
        String confLocaPath = "C:/Users/hu/Desktop/filter/conf2";
        String receptorLocalPATH = "C:/Users/hu/Desktop/filter/2RH1C2.pdbqt";
        String seed = "1351189036";
        HashMap<String, String> result =
                job.startJob(confLocaPath, receptorLocalPATH, test, seed, topK, vinaJobID);
        System.out.println(result.get("flag"));
        System.out.println(result.get("hadoopID"));
        System.out.println(result.get("vinaJobID"));
        System.out.println(result.get("log"));

      
    }
}
