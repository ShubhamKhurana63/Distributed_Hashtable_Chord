package edu.buffalo.cse.cse486586.simpledht;

import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

import java.io.IOException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.InetAddress;

import java.util.*;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.*;
import java.util.concurrent.Executor;

public class SimpleDhtProvider extends ContentProvider {


    class PortHashMapper
    {
        String port;
        String hash;

        public PortHashMapper()
        {

        }

        public String getPortNum(String port)
        {

            Map<String,String> m1=new HashMap<String, String>();
            m1.put("11108","5554");
            m1.put("11112","5556");
            m1.put("11116","5558");
            m1.put("11120","5560");
            m1.put("11124","5562");

            return m1.get(port);

        }


        public PortHashMapper(String port)
        {
            try
            {
                this.port=port;

                String portNum=getPortNum(port);

                this.hash=genHash(portNum);
            }catch(NoSuchAlgorithmException ex)
            {
                ex.printStackTrace();
            }
        }
    }

    static final String MIN_SHA1="0000000000000000000000000000000000000000";

    static final String MAX_SHA1="ffffffffff"+"ffffffffff"+"ffffffffff"+"ffffffffff";


    static PortHashMapper predecessor;

    static PortHashMapper successor;

    static PortHashMapper self;

    static String dataDelegate;
    static String selectionDelegate;




    class ConveyorTask extends AsyncTask<String,Void,Void>
    {
        @Override
        protected Void doInBackground(String... strings) {

            String toBeSent=strings[0];
            String ackExpected=strings[1];
            try
            {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(successor.port));
                BufferedReader bufferedReader=null;
                OutputStream os=null;
                InputStream is=null;
                if(socket.isConnected())
                {
                    os=socket.getOutputStream();
                    is=socket.getInputStream();
                    PrintWriter printWriter=new PrintWriter(os);
                    printWriter.print(toBeSent+"\n");//writing the data on socket
                    printWriter.flush();
                    //ACK Receiving Logic
                    bufferedReader=new BufferedReader(new InputStreamReader(is));
                    String  ackString=bufferedReader.readLine();
                    if(ackString!=null && ackString.equals(ackExpected))
                    {
                        socket.close();//closing socket, once the acknowledgement is received
                    }
                }
            }catch(UnknownHostException ex)
            {
                ex.printStackTrace();
                Log.e("==Insert UH EXCEPTION==",""+ex.getMessage());
            }catch (IOException ex)
            {
                ex.printStackTrace();
                Log.e("==Insert IO EXCEPTION==",""+ex.getMessage());
            }
            return null;
        }
    }



    public boolean deleteOnSelection(String selection)
    {
            boolean flag=false;
            Context context=getContext();
            File file=context.getFileStreamPath(selection);

            if(file.exists())
            {
                file.delete();
                flag=true;
            }
        return flag;
    }


    public boolean deleteLocal(Context context)
    {
        boolean flag=false;
        File directory=context.getFilesDir();
        if(directory.isDirectory())
        {
            Log.e("<==DEL DIRECTORY==>",""+flag);
            File[] files=directory.listFiles();
            if(files.length>0)
            {
                for(File file:files)
                {
                    file.delete();
                }
                flag=true;
            }
        }
        Log.e("<==DELETED LOCAL==>",""+flag);
        return flag;
    }





    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        //deleting from the DHT and the connecetd  DHT
        //deletion from * is also possible
        Log.e("<==IN DELETE==>",selection);

    if(selection.equals("*")||selection.equals("@"))
    {
            Context context=getContext();
            boolean flag=deleteLocal(context);
            Log.e("<==DELETED==>",""+flag);
            if(successor!=null && selection.equals("*"))
            {
            new ConveyorTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,"Delete*:"+"|"+self.port,"DELETE-ACK");
            }
    }else{
        deleteOnSelection(selection);
    }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }


    public boolean insertData(String fileName,String value,Context context)
    {
        boolean flag=false;


        Log.e("<===FILE STATUS======>",""+context.toString());

        File file=context.getFileStreamPath(fileName);
        Log.e("==PATH==",file.getAbsolutePath());
        //Log.d("",context.getFilesDir().toString());
        FileWriter fileWriter=null;
        try {
            try
            {
                if(!file.exists())//if file does not exists then at that point creating new one, else overwriting
                {
                    file=new File(context.getFilesDir(),fileName);
                }
                fileWriter=new FileWriter(file);
                fileWriter.write(value+"\n");
                fileWriter.flush();
                flag=true;
            }finally {
                //Log.d("<===FILE STATUS======>",""+file.exists());
                fileWriter.close();
            }
        }catch (IOException ex) {
            ex.printStackTrace();
        }
        Log.e("<===FILE STATUS======>",""+flag);
     return flag;
    }

    public boolean verifyRange(String hashOfKey)
    {
        int predNum=compareStrings(predecessor.hash,hashOfKey);//comparing hash with the
        int selfNum=compareStrings(self.hash,hashOfKey);
        int selfToPred=compareStrings(self.hash,predecessor.hash);//same scale

        boolean flag=false;
        if(selfToPred>0 && predNum<0 && selfNum>=0)//checking if the value is in the range
        {
            Log.e("====1====",""+selfToPred);
            return true;
        }

        if(selfToPred<0)
        {
            int minToSelf=compareStrings(self.hash,MIN_SHA1);//successor is always clockwise

            int maxToPred=compareStrings(predecessor.hash,MAX_SHA1);//so pred should be behind the max

            if(maxToPred<0 && minToSelf>0)
             {
                 //check if it lies between the min and self

                 int minToHash=compareStrings(MIN_SHA1,hashOfKey);
                 int hashToSelf=compareStrings(hashOfKey,self.hash);

                 if(minToHash<=0 && hashToSelf<=0)
                 {
                     Log.e("====2====",""+selfToPred);
                     return true;
                 }

                 //check if it lies between the predecessor and max
                 int maxToHash=compareStrings(MAX_SHA1,hashOfKey);
                 int hashToPred=compareStrings(hashOfKey,predecessor.hash);

                 if(maxToHash>=0 && hashToPred>0)
                 {
                     Log.e("====3====",""+selfToPred);
                     return true;
                 }
             }
            Log.e("====4====",""+selfToPred);
        }
        return flag;
    }


    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Context context=getContext();
        Set<String> keySet=values.keySet();
        Stack<String> stack=new Stack<String>();
        for(String s:keySet)
        {
            stack.push((String)values.get(s));
        }
        String fileName=stack.pop();
        String value=stack.pop();
        if(predecessor!=null && successor!=null)
        {
            try
            {
                String hashedKey=genHash(fileName);
                if(verifyRange(hashedKey))
                {
                    boolean bool=insertData(fileName,value,context);
                    Log.e("==DATA UP INSERTED==",""+bool);
                }else{
                    //if range is not verified
                            Log.e("==DATA UP INSERTED=="," ROLLING TASK");
                            String keyValueComb="Insert:"+"|"+fileName+"|"+value;
                            new ConveyorTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,keyValueComb,"INSERT-ACK");
                }
            }catch(NoSuchAlgorithmException ex)
            {
                ex.printStackTrace();
            }
        }else{
            boolean bool=insertData(fileName,value,context);
            Log.e("==DATA INSERTED==",""+bool);
        }

        //compare the self and the pred id(if found in the range) then insert
        // , if null then insert directly
        //if inserted and it's not in the range then pass it to successor, and the same process will follow
        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //action to trigger for the addition of the node
        return false;
    }


    public String getDumpFromLocal(Context context)
    {
        File directory=context.getFilesDir();
        String [] colNames= {"key","value"};
        MatrixCursor matrixCursor=new MatrixCursor(colNames);
        StringBuilder builder=new StringBuilder();
        if(directory.isDirectory())
        {
            File[] files=directory.listFiles();
            if(files.length>0)
            {
                for(File file:files)
                {
                    try
                        {
                            FileReader fileReader=null;
                            try
                            {
                                fileReader=new FileReader(file);
                                BufferedReader bufferedReader=new BufferedReader(fileReader);
                                String value=bufferedReader.readLine();
                                builder.append(file.getName());
                                builder.append(":");
                                builder.append(value);
                                builder.append("|");
                            }finally {
                                fileReader.close();
                            }
                        }catch (Exception ex)
                        {
                            ex.printStackTrace();
                        }
                }
            }
        }
        return builder.toString();
    }
    private MatrixCursor parseData(String dump)
    {
        String [] colNames= {"key","value"};
        MatrixCursor matrixCursor=new MatrixCursor(colNames);
       if(dump!=null&& dump.length()>0)
       {
           StringTokenizer tokenizer=new StringTokenizer(dump,"|");
           while(tokenizer.hasMoreTokens())
           {
               StringTokenizer colonTokenizer=new StringTokenizer(tokenizer.nextToken(),":");
               if(colonTokenizer!=null)
               {
                   String[] array=new String[2];
                   array[0]=colonTokenizer.nextToken();
                   array[1]=colonTokenizer.nextToken();
                   matrixCursor.addRow(array);
               }
           }
       }
        return matrixCursor;
    }


class GlobalDump extends AsyncTask<String,Void,Void>
{

    @Override
    protected Void doInBackground(String... strings) {
        try
        {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(successor.port));
            Log.e("====GlobalDump====","=============");
            PrintWriter printWriter=new PrintWriter(socket.getOutputStream());
            printWriter.print("Query*:"+"|"+self.port+"\n");
            printWriter.flush();
            BufferedReader reader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String dump=reader.readLine();
            Log.e("====GlobalDump====","============="+dump);
            if(dump!=null)
            {
                dataDelegate=dump;
                socket.close();
            }
        }catch (Exception ex)
        {
            ex.printStackTrace();
        }
        return null;
    }

}


class OnSelection extends AsyncTask<String,Void,Void>
{

    @Override
    protected Void doInBackground(String... strings) {
        try
        {
            String selection=strings[0];
            Log.e("==Entering ASYNC",selection);
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(successor.port));
            PrintWriter printWriter=new PrintWriter(socket.getOutputStream());
            printWriter.print("QryOnSel:"+"|"+self.port+"|"+selection+"\n");
            printWriter.flush();
            BufferedReader reader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String dump=reader.readLine();
            Log.e("====OnSelection====","BEFORE RECEIVING");
            if(dump!=null)
            {
                Log.e("====OnSelection====","AFTER RECEIVING");
                selectionDelegate=dump;
            }
            Log.e("====OnSelection====","OUT FROM RECEIVING");
            socket.close();
        }catch (Exception ex)
        {
            ex.printStackTrace();
        }

        return null;
    }
}





public String onSelectionQuery(String selection,Context context)
{
    String finalSel=null;
    try
    {
        File file=context.getFileStreamPath(selection);
        if(file.exists())
        {
            FileReader fileReader=new FileReader(file);
            BufferedReader bufferedReader=new BufferedReader(fileReader);
            String value=bufferedReader.readLine();
            finalSel=selection+":"+value;
        }

    }catch (Exception ex)
    {
        ex.printStackTrace();
    }
    return finalSel;
}

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        //for self things are fine
        //but if it comes on the server then we need to trigger the successor
        // linkups in a sequence till the point we reach the


        //if the id sent is the successor then don't give the request to successor

        /**
         *Line======================================139===========================
         * Resource-1:https://developer.android.com/training/data-storage/files
         *
         */
        Context context=getContext();
        if(selection.equals("@"))
        {
             String dump= getDumpFromLocal(context);
             return parseData(dump);
        }

        if(successor!=null  && selection.equals("*"))
        {
            new GlobalDump().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,null,null);
            while(true)
            {
                if(dataDelegate!=null)
                {
                    break;
                }
            }
            String dump= getDumpFromLocal(context);
            MatrixCursor matrixCursor= parseData(dump+dataDelegate);
            Log.e("==In Query==",dataDelegate);
            dataDelegate=null;
            return matrixCursor;
        }else if(successor==null && selection.equals("*"))
        {
            String dump= getDumpFromLocal(context);
            return parseData(dump);
        }

        File file=context.getFileStreamPath(selection);
        String [] colNames= {"key","value"};
        MatrixCursor matrixCursor=new MatrixCursor(colNames);
        FileReader fileReader=null;
        Log.e("query", selection);
        if(file.exists())//checking if the local file exists
        {
            try
            {
                try
                {
                    fileReader=new FileReader(file);
                    BufferedReader bufferedReader=new BufferedReader(fileReader);
                    String value=bufferedReader.readLine();
                    String[] array=new String[2];
                    array[0]=selection;
                    array[1]=value;
                    matrixCursor.addRow(array);
                }finally {
                    fileReader.close();
                }
            }catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }

        if(successor!=null && matrixCursor.getCount()==0)
        {
            Log.e("Query others","===finding in another AVD's====");
            new OnSelection().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR,selection);
            while(true)
            {
                if(selectionDelegate!=null)
                {
                    if(selectionDelegate.equals("NOT FOUND"))
                    {

                        Log.e("==NOT FOUND==",selection);
                    }else{
                        Log.e("Query others","===DATA CAME====");
                        matrixCursor= parseData(selectionDelegate+"|");
                    }
                    Log.e("==In Query==",selectionDelegate);
                    selectionDelegate=null;
                    break;
                }
            }
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }



    private int compareStrings(String parent,String arg)
    {
        return parent.compareTo(arg);
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






}
