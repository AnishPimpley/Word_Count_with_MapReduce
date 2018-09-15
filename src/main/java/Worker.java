// Java implementation for a client
// Save file as Client.java
//
//import java.io.*;
//import java.net.*;
//import java.util.Random;
//import java.util.Scanner;
//
//// Client class
//public class mc
//{
//    public static void main(String[] args) throws IOException
//    {
//        try
//        {
//            Scanner scn = new Scanner(System.in);
//
//            // getting localhost ip
//            InetAddress ip = InetAddress.getByName("localhost");
//
//            // establish the connection with server port 5056
//            Socket s = new Socket();
//            s.connect(new InetSocketAddress(ip, 5056));
//
//            InputStream is = s.getInputStream();
//            System.out.println(is.);
//
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//    }
//}

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
//import java.util.stream.Collectors;

// Client class
public class Worker {
    String p_ID ;
    String status;
    public Worker(){
        this.p_ID = "";
        this.status = "alive";
    }

    private void set_p_ID(String new_p_ID){ //set in 1st line of main, before starting other threads
        this.p_ID = new_p_ID;
    }

    private void heartbeat(){

        try
        {
            //this.wait(3000L);
            // establish the connection with server port 5056
            Socket s = new Socket("localhost", 4000);

            // obtaining input and out streams
            DataInputStream dis = new DataInputStream(s.getInputStream());
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());

            //dos.writeUTF("random");
            // the following loop performs the exchange of
            // information between client and client handler
            while (true)
            {
                String server_msg = dis.readUTF(); //wait for message from master
                dos.writeUTF(this.p_ID); // once recieved write out it's p_ID to master

                if(server_msg.equals("dead")){
                    break; //will receive dead from master if socket exception is encountered on the master side
                }

                if(this.status.equals("dead")){
                    break; // will receive dead if master tells the other thread, that all files have been processed.
                }

            }
            System.exit(0); // if break out due to being considered dead, then kill process

        }catch(Exception e){
        }
    }

    private void work()
    {
        try
        {

            String work_file = "";
            // establish the connection with server port 5056
            Socket s2 = new Socket("localhost", 5000);

            // obtaining input and out streams
            DataInputStream dis = new DataInputStream(s2.getInputStream());
            DataOutputStream dos = new DataOutputStream(s2.getOutputStream());

            while(true)
            {
                dos.writeUTF("idle," + this.p_ID); // first worker pings master for work
                work_file = dis.readUTF(); //master sends filename to work on

                if(work_file.equals("done")) // if no more file, then break out and set shared variable to dead, so heartbeat also breaks out
                {
                    this.status = "dead";
                    break;
                }

                //String work_file_address = ("E:\\590S_submission\\project-1-fall-2017-distributed-wordcount-AnishPimpley\\src\\test\\resources\\" + work_file);
                String work_file_address = ("./src/test/resources/" + work_file); // reads file
                String content = new Scanner(new File(work_file_address)).useDelimiter("\\Z").next();
                content = content.replace("\n",""); //remove line breaks
                String[] words = content.split(" "); //split on space bats
                HashMap<String, Integer> word_count = new HashMap<>(); //count in hash map
                for (String i : words)
                {
                    if(!word_count.containsKey(i)){
                        word_count.put(i,1);
                    }else{
                        word_count.put(i, word_count.get(i) + 1);
                    }
                }
                HashMap<String, Integer> sorted_hmap = new HashMap<>();
                word_count.entrySet().stream().sorted((k1, k2) -> -k1.getValue().compareTo(k2.getValue())).forEach(k -> sorted_hmap.put(k.getKey(), k.getValue())); //sort intermediate

                //FileOutputStream fos = new FileOutputStream("E:\\590S_submission\\project-1-fall-2017-distributed-wordcount-AnishPimpley\\src\\test\\resources\\"
                //        + work_file.replace(".txt","") + "_out.ser");

                //write out serialized file to memory

                FileOutputStream fos = new FileOutputStream("./src/test/resources/"
                        + work_file.replace(".txt","") + "_out.ser");
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                oos.writeObject(sorted_hmap);
                oos.close();
                fos.close();

            }

            System.exit(0);

        }catch(Exception e)
        {
        }
    }

    public void run(){
        Thread t = new Thread(() -> this.heartbeat());
        Thread t2 = new Thread(() -> this.work());
        t.start();
        t2.start();
    }

    public static void main(String[] args) throws IOException {

        Worker machine = new Worker();
        machine.set_p_ID(args[0]);
        machine.run();
    }
}