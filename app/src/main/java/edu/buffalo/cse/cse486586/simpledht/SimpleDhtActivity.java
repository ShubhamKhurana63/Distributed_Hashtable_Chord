package edu.buffalo.cse.cse486586.simpledht;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;
import java.io.*;
import android.net.Uri;
import android.util.Log;
import android.content.ContentResolver;
import android.content.ContentValues;
import java.net.Socket;

import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import android.telephony.TelephonyManager;
import java.net.InetAddress;
import java.util.concurrent.ThreadPoolExecutor;

import android.view.View;
import android.widget.EditText;







import android.content.Context;

public class SimpleDhtActivity extends Activity {



    private class Receiver extends  AsyncTask<String,Void,Void>
    {

        @Override
        protected Void doInBackground(String... strings) {

            String myPort=strings[0];
            try
            {
                String portNum=numPortMap.get("5554");
                Log.e("Starting PORT",portNum);
                Socket includeMeSocket =new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portNum));

                PrintWriter printWriter=new PrintWriter(includeMeSocket.getOutputStream());
                printWriter.print("Include Me:"+"|"+myPort+"\n");
                printWriter.flush();

                BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(includeMeSocket.getInputStream()));
                String ack=bufferedReader.readLine();

                if(ack!=null && ack.equals("INCLUDED"))
                {
                    includeMeSocket.close();
                }
                if(SimpleDhtProvider.successor!=null && SimpleDhtProvider.predecessor!=null)
                {
                    Log.e("======SUCCESSOR",""+SimpleDhtProvider.successor.port);
                    Log.e("======PREDECESSOR",""+SimpleDhtProvider.predecessor.port);
                }

            }catch (UnknownHostException ex)
            {
                ex.printStackTrace();
            }catch(IOException e)
            {
                e.printStackTrace();
            }
            return null;
        }
    }







    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final Integer SERVER_PORT=10000;

    Map<String,String> numPortMap=new HashMap<String, String>();
    Map<String,String> portNumMap=new HashMap<String, String>();
    SimpleDhtProvider simpleDhtProvider=null;




    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));
        numPortMap.put("5554","11108");
        numPortMap.put("5556","11112");
        numPortMap.put("5558","11116");
        numPortMap.put("5560","11120");
        numPortMap.put("5562","11124");

        portNumMap.put("11108","5554");
        portNumMap.put("11112","5556");
        portNumMap.put("11116","5558");
        portNumMap.put("11120","5560");
        portNumMap.put("11124","5562");

        try
        {
            simpleDhtProvider=new SimpleDhtProvider();
            TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
            String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
            SimpleDhtProvider.self=simpleDhtProvider.new PortHashMapper(myPort);
            Log.e(TAG, " create a ServerSocket");

            ServerSocket serverSocket=new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,serverSocket);//spawning the server thread, as the application launches

            Log.e(TAG, "Can create a ServerSocket");
            //include me
            if(!myPort.equals("11108"))
            {
                Log.e(TAG, "Inside the call");
                new Receiver().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,myPort);
            }



           /* //Dummy Implementation
            findViewById(R.id.button1).setOnClickListener(
                    new View.OnClickListener() {
                        @Override
                        public void onClick(View v) {
                            Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                            ContentResolver mContentResolver=getContentResolver();

                           // if(myPort.equals("11112"))
                            {
                                mContentResolver.delete(mUri,"*",null);
                            //}else{
                               *//* ContentValues contentValues=new ContentValues();
                                contentValues.put("key","5554");
                                contentValues.put("values","smile");
                                mContentResolver.insert(mUri,contentValues);

                             *//**//*   ContentValues contentValues2=new ContentValues();
                                contentValues.put("key","5556");
                                contentValues.put("values","smile");
                                mContentResolver.insert(mUri,contentValues);*//**//*


                                ContentValues contentValues3=new ContentValues();
                                contentValues.put("key","5562");
                                contentValues.put("values","smile");
                                mContentResolver.insert(mUri,contentValues);*//*


                             *//*   //ContentValues contentValues2=new ContentValues();
                                contentValues.put("key","5560");
                                contentValues.put("values","smile");
                                mContentResolver.insert(mUri,contentValues);

                                //ContentValues contentValues2=new ContentValues();

                                contentValues.put("key","5562");
                                contentValues.put("values","smile");
                                mContentResolver.insert(mUri,contentValues);*//*

                            }
                        }
                    }
            );*/



            findViewById(R.id.button1).setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                    ContentResolver mContentResolver=getContentResolver();
                    //String []keys={"5554","5556","5558","5560","5562"};
                    //Cursor resultCursor=mContentResolver.query(mUri, null, "@", null, null);

                    mContentResolver.delete(mUri,"*",null);

                    /*if (resultCursor == null)
                        Log.e(TAG, ">>>>>>>>>>>>>>>>>>>|||||||||<<<<<<<<<<<<<<<<<"+resultCursor.getCount());*/
                }
            });

            //Dummy Implementation
            findViewById(R.id.button2).setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                    ContentResolver mContentResolver=getContentResolver();

                    //String []keys={"5554","5556","5558","5560","5562"};

                    Cursor resultCursor=mContentResolver.query(mUri, null, "*", null, null);

                    if (resultCursor.getCount()==0)
                        Log.e(TAG, ">>>>>>>>>>>>>>>>>>>|||||||||<<<<<<<<<<<<<<<<<"+resultCursor.getCount());
                }
            });










        }catch (IOException ex)
        {
            ex.printStackTrace();
            Log.e(TAG, "Can't create a ServerSocket");
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String fetchGlobalDump(String haltPort)
    {
     String finalDump=null;
        try
        {
            Socket linkingSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(SimpleDhtProvider.successor.port));

            PrintWriter printWriter=new PrintWriter(linkingSocket.getOutputStream());
            printWriter.print("Query*:"+"|"+haltPort+"\n");
            printWriter.flush();

            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(linkingSocket.getInputStream()));
            String dump= bufferedReader.readLine();
            if(dump!=null)
            {
                finalDump=dump;
            }
            linkingSocket.close();
        }catch(Exception ex)
        {

        }finally {

        }
    return finalDump;
    }







    private boolean conveyAVD(String port,String message,String ack,String purpose)
    {
        boolean flag=false;
        try
        {
            Log.e("Inside CAVD",""+purpose);
            Log.e("CAVD PORT",""+port);
            Socket linkingSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(port));
            PrintWriter printWriter=new PrintWriter(linkingSocket.getOutputStream());

            printWriter.print(message+"\n");
            printWriter.flush();

            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(linkingSocket.getInputStream()));

            String receivedAck=bufferedReader.readLine();

            Log.e("CAVD ACK",""+receivedAck);
            if(receivedAck!=null && receivedAck.equals(ack))
            {
                Log.e("CAVD ACK","=====ACKNOWLEDGED=====");
                flag=true;
                linkingSocket.close();
                Log.e("Include Me",""+flag);
            }

        }catch(UnknownHostException ex)
        {
            ex.printStackTrace();
        }catch(IOException ex)
        {
            ex.printStackTrace();
        }
    return flag;
    }


    private String fetchDataOnSelection(String data)
    {
        String finalDump=null;
        try
        {
            Socket linkingSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(SimpleDhtProvider.successor.port));

            PrintWriter printWriter=new PrintWriter(linkingSocket.getOutputStream());
            printWriter.print(data+"\n");
            printWriter.flush();

            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(linkingSocket.getInputStream()));
            String dump= bufferedReader.readLine();
            if(dump!=null)
            {
                finalDump=dump;
            }
            Log.e("==In QryOnSel==","SELECTED DATA"+finalDump);
            linkingSocket.close();
        }catch(Exception ex)
        {
        ex.printStackTrace();

        }finally {

        }
        return finalDump;
    }










    private class ServerTask extends AsyncTask<ServerSocket,Void,Void>
    {
        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];
            Log.e("Socket Established",">>>>>>>>>>>>>");

            try
            {
                PrintWriter printWriter=null;
                BufferedReader bufferedReader=null;
                try
                {
                    while(true)//infinite loop ensures that server thread is always up and running
                    {

                        if(SimpleDhtProvider.successor!=null && SimpleDhtProvider.predecessor!=null)
                        {
                            Log.e(">>>>>SUCCESSOR",""+SimpleDhtProvider.successor.port);
                            Log.e(">>>>>PREDECESSOR",""+SimpleDhtProvider.predecessor.port);
                        }

                        Socket socket=serverSocket.accept();//blocking call, where server waits for the client to establish connection & send data
                        InputStream is=socket.getInputStream();
                        OutputStream os=socket.getOutputStream();

                        bufferedReader=new BufferedReader(new InputStreamReader(is));

                        Log.e("Socket Established",">>>>>>>>>>>>>"+socket.isConnected());
                        String data=bufferedReader.readLine();
                        if(data!=null && data.contains("Include Me:"))
                        {
                            Log.e("Include Me","socket.isConnected()");
                            StringTokenizer tokenizer=new StringTokenizer(data,"|");
                            tokenizer.nextToken();
                            String toBeIncludedPort=tokenizer.nextToken();

                            printWriter=new PrintWriter(socket.getOutputStream());
                            printWriter.print("INCLUDED"+"\n");
                            printWriter.flush();
                            socket.close();


                            //when stand alone only exists
                            if(SimpleDhtProvider.successor==null && SimpleDhtProvider.predecessor==null)
                            {
                                Log.e("Deciding Chord","socket.isConnected()");
                                SimpleDhtProvider.successor=simpleDhtProvider.new PortHashMapper(toBeIncludedPort);
                                SimpleDhtProvider.predecessor=simpleDhtProvider.new PortHashMapper(toBeIncludedPort);
                                String message="Inchord"+"|"+SimpleDhtProvider.self.port+"|"+SimpleDhtProvider.self.port;
                                Log.e("Deciding Port",toBeIncludedPort);

                                conveyAVD(toBeIncludedPort,message,"INCLUDED"," SENDING PRED AND SUCC ");
                                //incomplete logic for pumping-------------//

                                /*String localDump=simpleDhtProvider.getDumpFromLocal(getApplicationContext());

                                StringTokenizer dumpTokenizer=new StringTokenizer(localDump,"|");
                                String finalDump="Shift:*"+"|";
                                Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                                ContentResolver mContentResolver=getContentResolver();


                                while (dumpTokenizer.hasMoreTokens())
                                {
                                    StringTokenizer pairTokenizer=new StringTokenizer(dumpTokenizer.nextToken(),":");
                                    String key=pairTokenizer.nextToken();
                                    String value=pairTokenizer.nextToken();
                                    boolean flag=simpleDhtProvider.verifyRange(genHash(key));
                                    if(!flag)
                                    {
                                      mContentResolver.delete(mUri,key,null);
                                      finalDump=finalDump+key+":"+value+"|";
                                    }
                                }*/

                                //greater than the self and

                            }else if(SimpleDhtProvider.successor!=null && SimpleDhtProvider.predecessor!=null)
                            {
                                String hash=genHash(portNumMap.get(toBeIncludedPort));
                                boolean flag=simpleDhtProvider.verifyRange(hash);
                                if(flag)
                                {
                                    String message="Inchord"+"|"+SimpleDhtProvider.self.port+"|"+SimpleDhtProvider.predecessor.port;
                                    conveyAVD(toBeIncludedPort,message,"INCLUDED"," SENDING PRED AND SUCC ");
                                    //update pred to update its succesor
                                    //convey to himself

                                    String changePredecessor="ChangePred:"+"|"+toBeIncludedPort;
                                    conveyAVD(SimpleDhtProvider.predecessor.port,changePredecessor,"SUCCESSOR CHANGED"," Change Predecessor ");


                                    SimpleDhtProvider.predecessor=simpleDhtProvider.new PortHashMapper(toBeIncludedPort);

                                }else{
                                    conveyAVD(SimpleDhtProvider.successor.port,data,"INCLUDED"," Forward For Joining");
                                }
                            }
                                //for updating successor and predecessor of the node
                        }else if(data!=null && data.contains("Inchord"))
                        {
                            StringTokenizer stringTokenizer=new StringTokenizer(data,"|");
                            stringTokenizer.nextToken();
                            SimpleDhtProvider.successor=simpleDhtProvider.new PortHashMapper(stringTokenizer.nextToken());
                            SimpleDhtProvider.predecessor=simpleDhtProvider.new PortHashMapper(stringTokenizer.nextToken());

                            if(SimpleDhtProvider.successor!=null && SimpleDhtProvider.predecessor!=null)
                            {
                                Log.e("<<<<<SUCCESSOR",""+SimpleDhtProvider.successor.port);
                                Log.e("<<<<<PREDECESSOR",""+SimpleDhtProvider.predecessor.port);
                            }

                            printWriter=new PrintWriter(socket.getOutputStream());
                            printWriter.print("INCLUDED"+"\n");
                            printWriter.flush();
                            socket.close();
                        }//changing successor in predecessor
                        else if(data!=null && data.contains("ChangePred:"))
                        {
                            StringTokenizer stringTokenizer=new StringTokenizer(data,"|");
                            stringTokenizer.nextToken();
                            SimpleDhtProvider.successor=simpleDhtProvider.new PortHashMapper(stringTokenizer.nextToken());

                            printWriter=new PrintWriter(socket.getOutputStream());
                            printWriter.print("SUCCESSOR CHANGED"+"\n");
                            printWriter.flush();
                            socket.close();
                        }
                        //for insertion
                        else if(data!=null && data.contains("Insert:"))
                        {
                            Log.e("<<<<<Inside Insert",""+data);
                            printWriter=new PrintWriter(new OutputStreamWriter(os));
                            String ackString="INSERT-ACK"+"\n";
                            printWriter.print(ackString);
                            printWriter.flush();
                            socket.close();

                            Log.e("<<<<<Inside Insert",""+data);
                            StringTokenizer tokenizer=new StringTokenizer(data,"|");
                            tokenizer.nextToken();
                            String fileName=tokenizer.nextToken();
                            String value=tokenizer.nextToken();
                            //simpleDhtProvider
                            String hashOfKey=genHash(fileName);

                            if(simpleDhtProvider.verifyRange(hashOfKey))
                            {
                                Log.e("<<<<<Insert",""+data);
                                Log.e(">>>file-value<<<",fileName+"|"+value);
                                simpleDhtProvider.insertData(fileName,value,getApplicationContext());
                            }
                            else
                            {
                                Log.e("<<<<<Forwarded to: ",""+SimpleDhtProvider.successor.port);
                                boolean flag=conveyAVD(SimpleDhtProvider.successor.port,data,"INSERT-ACK","Insertion in Process");
                                Log.e("Insert Request ","==Forwarded==");
                            }//for deletion
                        }else if(data!=null && data.contains("Delete*:"))
                        {
                            printWriter=new PrintWriter(os);
                            printWriter.write("DELETE-ACK"+"\n");
                            printWriter.flush();
                            socket.close();

                            StringTokenizer tokenizer=new StringTokenizer(data,"|");
                            tokenizer.nextToken();
                            String haltPort=tokenizer.nextToken();
                            Context context=getApplicationContext();
                            boolean flag=simpleDhtProvider.deleteLocal(context);

                            Log.e("<==DIRECT DELETE===>",""+flag);
                            if(!SimpleDhtProvider.successor.port.equals(haltPort))
                            {
                                conveyAVD(SimpleDhtProvider.successor.port,data,"DELETE-ACK","DELETE ALL REQUEST");
                            }//for query
                        }else if(data!=null && data.contains("Query*:"))
                        {
                            Log.e("==In Query==","ENTERING");
                            StringTokenizer tokenizer=new StringTokenizer(data,"|");
                            tokenizer.nextToken();
                            String haltPort=tokenizer.nextToken();
                            String fetchedDump="";
                            if(!SimpleDhtProvider.successor.port.equals(haltPort))
                            {
                            fetchedDump=fetchGlobalDump(haltPort);
                            }
                            String fetchedLocalDump=simpleDhtProvider.getDumpFromLocal(getApplicationContext());

                            String finalDump=fetchedLocalDump+fetchedDump+"\n";
                            Log.e("==In Query==",finalDump);
                            printWriter=new PrintWriter(socket.getOutputStream());
                            printWriter.print(finalDump);
                            printWriter.flush();
                            socket.close();
                        }else if(data!=null && data.contains("QryOnSel:"))
                        {
                            Log.e("==In QryOnSel==","ENTERING");
                            StringTokenizer tokenizer=new StringTokenizer(data,"|");
                            tokenizer.nextToken();
                            String haltPort=tokenizer.nextToken();
                            String selection=tokenizer.nextToken();
                            printWriter=new PrintWriter(socket.getOutputStream());

                            String selected=simpleDhtProvider.onSelectionQuery(selection,getApplicationContext());

                            Log.e("==In QryOnSel=="," DATA CAME");

                            if(SimpleDhtProvider.successor.port.equals(haltPort) && selected==null)
                            {
                               selected="NOT FOUND";
                            }else if(selected==null)
                            {
                                Log.e("==In QryOnSel=="," CALL FORWARDED");
                                selected=fetchDataOnSelection(data);
                            }
                            Log.e("==In QryOnSel==","SELECTED DATA"+selected);
                            printWriter.print(selected+"\n");
                            printWriter.flush();
                            socket.close();
                        }
                    }

                }finally {
                    //closing the resources after the processing is done
                    if(printWriter!=null)
                    {
                        printWriter.close();
                    }
                    if(bufferedReader!=null) {
                        bufferedReader.close();
                    }
                }
            }catch (IOException e) {
                e.printStackTrace();
                //Log.e(TAG, "=========reading data from socket failed====");
            }catch(NoSuchAlgorithmException ex)
            {
                ex.printStackTrace();
            }
            return null;
        }
    }




}
