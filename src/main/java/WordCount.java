import jdk.nashorn.internal.runtime.ECMAException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 Master:
 Note: all shared variables are initialized as thread safe data structures

 Word count is working for the code as can be found in Wordcount.main()
 Works for any combination of Number of workers and number of files.
 See src/test/resources/Final_result.txt to see results of running word count for random.txt & simple.txt

 the full process flow of master and worker are drawn in README.md

 */
public class WordCount implements Master {
    int num_workers;
    List<String> pID_list ; // list of ID's of currently active processes
    List<Process> processlist ; // list of processes that were initialized.
    ConcurrentLinkedQueue<String> pending_queue ; // queue of files that have not yet been processed. Starts full
    ConcurrentHashMap<String, String> pID_file_map; // map from what process (p_ID) is working on which file. Only shows active processes
    List<String> output_files ; // list of files that have completed processing with the output file names
    Iterator pID_setter; // iterator that sets p_ID for a process and moves pointer ahead (so no 2 p_ID's are the same)
    int num_files; //number of files, can be final
    PrintStream print_out; // printstream to which final output is redirected

    public WordCount(int workerNum, String[] filenames) throws IOException {
        this.num_workers = workerNum;
        this.num_files = filenames.length;

        this.pending_queue = new ConcurrentLinkedQueue<>();
        for(String name : filenames){   // fill queue
            pending_queue.add(name);
        }
        this.pID_setter = Helpers.range_iterator(5,5 + 100); // just need to be a large range. actual parameters not important

        //thread safe data structures
        this.pID_file_map = new ConcurrentHashMap<>();
        this.pID_list = Collections.synchronizedList(new ArrayList<String>());
        this.processlist = Collections.synchronizedList(new ArrayList<Process>());
        this.output_files = Collections.synchronizedList(new ArrayList<String>());
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"random.txt","simple.txt"}; //input argument (reads files for src/test/resources)
        WordCount master = new WordCount(2, args);
        master.setOutputStream(new PrintStream(System.out));
        master.run();
    }

    public void run() throws IOException {

        Thread combiner = new Thread(()-> this.combine()); //see program flow readme (system.exit in this thread)
        combiner.start();

        // 'n' workers are created
        try
        {
            for(int i=0; i< this.num_workers; i++)
            {
                this.createWorker(); // see create worker function for process creation details
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 2 Server sockets. port 4000 for heartbeats & port 5000 for handing over work
        ServerSocket ss = new ServerSocket(4000);
        ServerSocket ss2 = new ServerSocket(5000);

        while(this.output_files.size() != this.num_files){
            System.out.println("entered socket loop");

            Socket s = null;
            Socket s2= null;
            try //heart beat for every worker
            {
                // socket object to receive incoming client requests
                s = ss.accept(); //new heartbeat socket unique to that a worker-master pair
                s.setSoTimeout(6000); // sets a timeout on read operations by that socket. If not reply in 6 seconds...socket exception is thrown
                System.out.println("A new heartbeat client is connected : " + s);

                // obtaining input and out streams unique to a worker-master pair
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                // New thread deals with heart beat between 1 worker-master pair.
                // we will create 'n' such threads over 'n' iterations of parent while loop.
                Thread t = new Thread(()-> this.heartbeat(dis, dos));

                // Invoking the start() method
                t.start();

            }catch (Exception e){
                System.out.println("some exception");
            }

            try{// handing over work to the worker
                s2 = ss2.accept(); //new give_work socket unique to that a worker-master pair
                System.out.println("A new worker client is connected : " + s2);

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(s2.getInputStream());
                DataOutputStream dos = new DataOutputStream(s2.getOutputStream());

                // New thread deals with giving_work between 1 worker-master pair.
                // we will create 'n' such threads over 'n' iterations of parent while loop.
                Thread t = new Thread(()-> this.giveWork(dis, dos));
                t.start();

            }catch(Exception e){
                System.out.println("exception 2");
            }
        }

    }

    public void createWorker() throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = Worker.class.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(
                javaBin, "-cp", classpath, className, String.valueOf(this.pID_setter.next()));

        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);

        Process process = builder.start();
        this.processlist.add(process);
        /**
         Could not identify a way to directly get identification of process when created. Alternate method used as follows:
         P_ID setter adds some unique (P_ID) as a String argument to the main() of worker, when worker is created (line 125)
         when worker connects back to master, it sends the passed unique p_ID back as a reply to heartbeat.
         Master uses these reply p_IDs to heartbeat messages as an identifier for how many processes area alive, and uniquely identifying them.
        */
        // IGNORE COMMENTED OUT CODE
/*        List<String> command = new ArrayList<>() ;
        command.add("Java");
        command.add("worker");
        command.add(String.valueOf(this.pID_setter.next()));

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(new File("E:\\590S_submission\\project-1-fall-2017-distributed-wordcount-AnishPimpley\\src\\main\\java"));
        Process p = pb.start();
        System.out.println("worker created");*/
    }

    public void combine() {

        //this method is called by a new master thread at the very beginning of master.run();

        HashMap<String,Integer> word_count_all = new HashMap<>();

        while(true){
            try{
                //waits till the number of files outputted is equal to the number of files that needed processing.
                // ie. if all intermediate tasks are completed
                if(this.output_files.size() == this.num_files){
                    // iterates over every intermediate file
                    for(String file_name : this.output_files){
                        //FileInputStream streamIn = new FileInputStream("E:\\590S_submission\\project-1-fall-2017-distributed-wordcount-AnishPimpley\\src\\test\\resources\\" + file_name);

                        // Reads an intermediate serialized data structure (of type .ser), in this case a hash map

                        FileInputStream streamIn = new FileInputStream("./src/test/resources/" + file_name);
                        ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
                        HashMap<String,Integer> KV_pairs = (HashMap<String,Integer>) objectinputstream.readObject();

                        // Add counts from intermediate hash maps to central word count hash map.
                        for(String key : KV_pairs.keySet()){
                            if(word_count_all.containsKey(key)){
                                int count = word_count_all.get(key);
                                word_count_all.put(key, count + KV_pairs.get(key));
                            }else{
                                word_count_all.put(key,KV_pairs.get(key));
                            }
                        }

                    }

                    // once it has iterated over all files, it will break from the while loop

                    break; //goes to line 197
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }

        Map<String, Integer> sorted_hmap_all = new HashMap<>();
        //word_count_all.entrySet().stream().sorted((k1, k2) -> -k1.getValue().compareTo(k2.getValue())).forEach(k -> sorted_hmap_all.put(k.getKey(), k.getValue())); //IGNORE
        sorted_hmap_all = Helpers.sortByValue(word_count_all); //see helpers. Uses sort by value to reverse sort the hashmap

        try
        {
            // Write out results to a file (final_result) and the requested output stream.
            //FileWriter writer = new FileWriter("E:\\590S_submission\\project-1-fall-2017-distributed-wordcount-AnishPimpley\\src\\test\\resources\\final_result.txt"); //IGNORE
            FileWriter writer = new FileWriter("./src/test/resources/final_result.txt");
            for(String key: sorted_hmap_all.keySet())
            {
                writer.write(key + " : " +String.valueOf(sorted_hmap_all.get(key)) + "\n");
                this.print_out.printf(key + " : " +String.valueOf(sorted_hmap_all.get(key)) + "\n");
            }

            writer.close();
            //Once writing is done we can close master
            System.exit(0);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void heartbeat(DataInputStream dis, DataOutputStream dos) {

        String p_ID = "";
        String recieved;

        while (true)
        {
            try { //find a way to do a timeout exception

                dos.writeUTF("hellooo"); //First Master pings the worker
                //System.out.println("sent hello messages");

                // receive the answer from client (this will be his p_ID)
                recieved = dis.readUTF();

                //System.out.println(recieved + " : pID of what heartbeat got back");
                //check if this is the first time we are assigning p_ID a value:
                if(p_ID.equals("")){
                    //System.out.println(p_ID + "before printing p_ID list");
                    p_ID = recieved;
                    synchronized (this) {
                        this.pID_list.add(p_ID); //adds the new process ID to shared list of active processes
                        //System.out.println(this.pID_list);
                    }
                }

                synchronized(dos){//waits for 3 seconds before next ping
                    dos.wait(3000L);
                }

            } catch (IOException e) {
                // catches socket exception (child exception of IO exception) if the preset timeout of 6 secs is violated.
                // fault tolerance measures
                this.pID_list.remove(p_ID); // remove from active processes ID list
                if (this.pID_file_map.contains(p_ID)){
                    String filename = this.pID_file_map.get(p_ID);
                    this.pID_file_map.remove(p_ID); // remove from hashmap between process ID and file it was operating on
                    this.pending_queue.add(filename); //add the file back into the queue
                }
                try{
                    this.createWorker();
                    // now that  worker has died, we create a new worker.
                    // It will ping master's central thread in central while loop where Server Socket listener is waiting
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                break;

            } catch (InterruptedException i) {
                i.printStackTrace();
                break;
            }
        } // theoretically while loop will continue eternally (until completion) if worker at p_ID doesn't die.
        // If it does fail, it will break out of the loop/
        // In which case this method will complete and thread will kill itself.
        try
        {
            // closing resources
            dis.close();
            dos.close();

        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void giveWork(DataInputStream dis, DataOutputStream dos) {
        String worker_status = "busy";
        String filename = "";
        while (true)
        {
            try {
                //System.out.println("reading worker status"); IGNORE LINE

                // receives work request from worker
                // work request is of the form : "idle,p_ID"
                worker_status = dis.readUTF(); // no timeout here, is blocking read
                //System.out.println(worker_status + " : state and pID from worker in give work");

                // if filename is already assigned and worker gets idle, that means that the filename is done processing
                // so we add it to shared output_files data structure
                if(worker_status.contains("idle")){

                    if(!filename.equals("")){
                        this.output_files.add(filename.replace(".txt","") + "_out.ser");
                    }
                    // get the p_ID from the workers message, and assign the file polled from queue to the p_ID
                    // store this information is p_ID-filename mapper shared hashmap.
                    String p_ID = worker_status.split(",")[1];
                    synchronized (this) {
                        if (this.pending_queue.peek() != null) {
                            filename = this.pending_queue.poll();
                            this.pID_file_map.put(p_ID, filename);
                        }else{
                            break; // break if pending queue is empty
                        }
                    }

                    dos.writeUTF(filename); //write the intermediate file out to memory
                    //won't enter a new loop of giving work unless message : "idle" recieved again from worker
                    worker_status = "busy";
                }
            } catch (IOException e) {
            }
        }

        try
        {
            dos.writeUTF("done");
            // closing resources
            dis.close();
            dos.close();

        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public void setOutputStream(PrintStream out) {
        this.print_out = out;
    }

    public Collection<Process> getActiveProcess() {
        return this.processlist;
    }
}

