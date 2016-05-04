package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import static edu.buffalo.cse.cse486586.simpledynamo.DynamoConstants.*;


public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private final Uri providerUri = ProviderHelper.buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private static String[] cols = new String[] { DynamoConstants.KEY_FIELD, DynamoConstants.VALUE_FIELD };

	public static ConcurrentHashMap<String, String> portStatus = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, String> queryHelper = new ConcurrentHashMap<String, String>();
	public static ConcurrentSkipListMap<String, String> hashedPorts = new ConcurrentSkipListMap<String, String>();

	public static ConcurrentHashMap<String, HashMap<String,String>> deadMap = new ConcurrentHashMap<String, HashMap<String,String>>();
	public static ConcurrentHashMap<String, String> recoveryMap = new ConcurrentHashMap<String, String>();

	public static boolean waitFlag = false;
	public static boolean createFlag = false;
	public static boolean insertFlag = false;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		int returnValue = 0;
		try {
			if (selection.matches("\"*\""))
			{
				returnValue = writeToFile();
				if(selectionArgs == null)
				{
					//for(int i = 11108; i <= DynamoConstants.REMOTE_PORTS.size(); i+=4)
					for (String node_Counter : REMOTE_PORTS)
					{
						if (!portStatus.get(node_Counter).equals(STR_DEAD))
						{
							if (!node_Counter.equals(getPort()))
							{
								ClientTask client_task = new ClientTask();
								client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, STR_DELETE + "%%" + selection + "%#%", node_Counter);
							}
						}
					}
				}
			}
			else if(selection.matches("\"@\""))
			{
				returnValue = writeToFile();
				Log.v(TAG, "Deleting all data from current avd" + returnValue);
			}
			else
			{
				writeToFile();
			}
			Log.e(TAG, "Files deleted from avd : " + returnValue);
		}catch(Exception e){
			Log.e(TAG, "Delete: Failed" + e.getMessage());
		}
		return returnValue;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String filename = null;
		String fileHashed = null;
		String content = null;
		int countPref = 0;

		String thisPort = getPort();
		try {
			for( String key: values.keySet() ) {
				if (key.equals( "key" ) ){
					filename = (String)values.get(key);
					fileHashed = ProviderHelper.genHash(filename);
					Log.d(TAG, "Inserted key is:" + filename + "with hash_id" + fileHashed);
				} else {
					content = (String)values.get( key );
					Log.d(TAG, "Inserted value is:" + content);
				}
			}
			//Log.d(TAG, "Insert: Key = " + filename );

			//List<String> prefList = ProviderHelper.getPreferenceList(ProviderHelper.searchCoordinator(fileHashed), true);
			String coordinator = ProviderHelper.searchCoordinator(fileHashed);
			List<String> prefList = ProviderHelper.getPreferenceList(coordinator, true);
			//Log.d(TAG, "Insert: preference list size : " + prefList.size());
			if ( prefList.contains( thisPort ) ) {
				Date date = new Date();
				insertContent(filename, date.getTime() + "##" + content );
				prefList.remove(thisPort);
				Log.d(TAG, "Insert: Final size of priority queue: " + prefList.size());
			}
			for( String pref : prefList ) {
				if(!ProviderHelper.isDead(pref)) {
					countPref ++;
				}
			}
			for (String pref : prefList) {
				if (!ProviderHelper.isDead(pref)) {
					Log.d(TAG, "Insert: inserting " + filename + " @ " + pref );
					countPref--;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DynamoConstants.STR_INSERT + filename + "%%" + content + "%%" + countPref, pref);
				} else {
					Log.d(TAG, "Insert: " + pref + " missed : " + filename);
					Date date = new Date();
					if (!deadMap.containsKey(pref)) {
						HashMap<String, String> tempMap = new HashMap();
						tempMap.put(filename, date.getTime() + "##" + content);
						Log.d(TAG, "Inserted:" + filename + "value" + fileHashed);
						deadMap.put(pref, tempMap);
					} else {
						deadMap.get(pref).put(filename, date.getTime() + "##" + content);
						Log.d(TAG, "Insert missed:" + filename + "value" + fileHashed);
					}
				}
			}
			insertFlag = false;
			while (!insertFlag){

			}
			insertFlag = false;

		} catch ( Exception e) {
			Log.e(TAG, "Insert: Exception : " + e.getMessage() );
		}

		Log.v("insert", values.toString());
		return uri;
	}

	@Override
	public boolean onCreate() {

		try
		{
			try
			{
				Log.d(TAG, "onCreate: Creating a ServerSocket");
				ServerSocket server_socket = new ServerSocket(SERVER_PORT);
				ServerTask server_task = new ServerTask();
				server_task.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server_socket);
			} catch (IOException e) {
				Log.e(TAG, "onCreate: Failed to create a ServerSocket" + e.getMessage());
				e.printStackTrace();
				return false;
			}

			Thread newThread = new Thread()
			{
				@Override
				public void run()
				{
					int myCounter = 4;
					for (String input : REMOTE_PORTS)
					{
						try
						{
							String thisPort = String.valueOf(Integer.parseInt(input) / 2);
							hashedPorts.put(ProviderHelper.genHash(thisPort), input);
							Log.d(TAG, "OnCreate: my port" + thisPort + "with id: " + input);
							portStatus.put(input, STR_ALIVE);
							if (input.equals(getPort())==false)
							{
								myCounter--;
								Log.d(TAG, "Creating ClientTask");
								ClientTask client_task = new ClientTask();
								client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, STR_ALIVE_MSG + "%%" + getPort() + "%%" + Integer.toString(myCounter), input);
							}
						} catch (Exception e) {
							Log.e(TAG, "OnCreate: Error in recovery " + e.getMessage());
							e.printStackTrace();
						}
					}
					createFlag = false;
					while (!createFlag)
					{
						//do nothing
					}
					createFlag = false;
					for (Map.Entry<String, String> entry : recoveryMap.entrySet())
					{
						insertContent(entry.getKey(), entry.getValue());
					}
					recoveryMap = new ConcurrentHashMap();
					Log.d(TAG, "OnCreate: NodeRecovery size: " + recoveryMap.size());
				}
			};
			newThread.start();
		} catch (Exception e){
			Log.e(TAG, "onCreate:Exception in SimpleDynamoProvider " + e.getMessage());
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
	{
		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
		String result = null;
		FileInputStream inputStream;
		BufferedReader bufferedReader;
		String read_line;
		List<String> nodes_alive = new ArrayList<String>();
		try {
			if(selection.contains("*"))
			{
				Log.d(TAG, "Query: Selection is " + selection);
				if(selectionArgs == null)
				{
					synchronized (this)
					{
						Log.d(TAG, "Query: Check aliveness of nodes");
						fetchAllFiles();
						for (String port : portStatus.keySet())
						{
							if (portStatus.get(port).equals(STR_ALIVE))
							{
								nodes_alive.add(port);
							}
						}
						nodes_alive.remove(getPort());
						int prefSize = nodes_alive.size();
						for (String port : nodes_alive)
						{
							prefSize--;
							ClientTask client_task = new ClientTask();
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, STR_QUERY_GLOBAL + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}
						Log.d(TAG, "Query: waiting for messages with queue size" + prefSize);
						for (String key : queryHelper.keySet())
						{
							String values[] = queryHelper.get(key).split("##");
							values[0] = key;
							cursor.addRow(values);
						}
						Log.d(TAG, "Query: Global Fetching all files after wait");
					}
				}
				else
				{
					for (String file : getContext().fileList())
					{
						inputStream = getContext().openFileInput(file);
						bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
						while ((read_line = bufferedReader.readLine()) != null) {
							result = read_line;
						}
						String[] values = new String[]{file, result};
						cursor.addRow(values);
					}
				}
				return cursor;
			}
			else if(selection.matches( "@" ))
			{
				for (String file : getContext().fileList())
				{
					inputStream = getContext().openFileInput(file);
					bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
					while ((read_line = bufferedReader.readLine()) != null)
					{
						String[] tempSplit = read_line.split("##");
						result = tempSplit[1];
					}
					String[] values = new String[]{file, result};
					cursor.addRow(values);
				}
				return cursor;
			}
			else
			{
				if(selectionArgs == null)
				{
					synchronized (this)
					{
						List<String> prefList = ProviderHelper.getPreferenceList(ProviderHelper.searchCoordinator(ProviderHelper.genHash(selection)), false);
						Log.d(TAG, "Query: Retrieving files from Queue");
						Log.d(TAG, "Query: Number of files are " + prefList.size());
						int prefSize = prefList.size();
						for (String port : prefList)
						{
							prefSize--;
							ClientTask client_task = new ClientTask();
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, STR_QUERY_LOCAL + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}
						waitFlag = false;
						while (!waitFlag)
						{
							//do nothing
						}
						Log.d(TAG, "Local Query: Wait is over");
						if (queryHelper.get(selection) != null)
						{
							String tempSplit[] = queryHelper.get(selection).split("##");
							result = tempSplit[1];
						}
						if( result != null )
						{
							String[] values = new String[]{selection, result};
							cursor.addRow(values);
						}
						Log.d(TAG, "Query: Value returned is : " + result);
					}
				}
				else
				{
					try {
						inputStream = getContext().openFileInput(selection);
						bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
						Log.d(TAG, "Query: Acknowledging originator");
						while ((read_line = bufferedReader.readLine()) != null)
						{
							result = read_line;
						}
					} catch (IOException e) {
						Log.e(TAG, "Query IOException" + e.getMessage());
					} catch (Exception e) {
						Log.e(TAG, "Query: File missing error" + e.getMessage());
						e.printStackTrace();
					}
					if(result != null)
					{
						String[] values = new String[]{selection, result};
						cursor.addRow(values);
						Log.d(TAG, "Final returned files: " + cursor.getCount());
						Log.d(TAG, "Query: Result returned at" + " " + selection + result);
					}
				}
				Log.d(TAG, "Number of live nodes are " + nodes_alive.size());
				return cursor;
			}
		} catch (Exception e) {
			Log.e(TAG, "Query Exception" + e.getMessage());
			e.printStackTrace();
		}
		return cursor;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			HashMap<String,String> dummyMap = new HashMap();
			dummyMap.put( DynamoConstants.KEY_FIELD, DynamoConstants.VALUE_FIELD );
			String thisPort = getPort();
			String msgReceived;
			Socket socket;
			try {
				while( true ) {
					socket = serverSocket.accept();

					ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
					if ((msgReceived = (String)reader.readObject()) != null) {

						msgReceived = msgReceived.trim();
						ObjectOutputStream writer = new ObjectOutputStream( socket.getOutputStream() );

						if( msgReceived.contains( DynamoConstants.STR_ALIVE_MSG ) ) {
							String strReceiveSplit[] = msgReceived.split("%%");
							Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Alive" );
							if( portStatus.containsKey( strReceiveSplit[1] ) ) {
								portStatus.put(strReceiveSplit[1], DynamoConstants.STR_ALIVE);
							}
							writer.writeObject(deadMap.get(strReceiveSplit[1]));
							deadMap.remove(strReceiveSplit[1]);
							writer.close();

						} else if( msgReceived.contains( DynamoConstants.STR_DEAD_MSG ) ) {
							String strReceiveSplit[] = msgReceived.split("%%");
							Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Dead" );
							if( portStatus.containsKey( strReceiveSplit[1] ) ) {
								portStatus.put(strReceiveSplit[1], DynamoConstants.STR_DEAD);
							}
							writer.writeObject(dummyMap);

						} else if(msgReceived.contains(DynamoConstants.STR_DELETE)){
							String strReceiveSplit[] = msgReceived.split("%%");
							Log.d( TAG, "ServerTask: Deleting selection : " + strReceiveSplit[1] );
							getContext().getContentResolver().delete(providerUri, strReceiveSplit[1], strReceiveSplit);
							writer.writeObject(dummyMap);

						} else if(msgReceived.contains(DynamoConstants.STR_QUERY_GLOBAL) ){
							String strReceiveSplit[] = msgReceived.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							Cursor resultCursor = getContext().getContentResolver().query(providerUri, null, strReceiveSplit[1], sendSelectArgs, null);
							HashMap<String, String> returnValue = new HashMap<String, String>();
							try {
								if (resultCursor != null) {
									Log.d(TAG, "ServerTask: GQuerying @ " + thisPort + " returned " + resultCursor.getCount());
									int keyIndex = resultCursor.getColumnIndex(DynamoConstants.KEY_FIELD);
									int valueIndex = resultCursor.getColumnIndex(DynamoConstants.VALUE_FIELD);

									for (resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
										returnValue.put(resultCursor.getString(keyIndex), resultCursor.getString(valueIndex));
									}
									resultCursor.close();
								}
							}catch( Exception e ){}
							writer.writeObject(returnValue);
						} else if(msgReceived.contains(DynamoConstants.STR_QUERY_LOCAL)){
							Log.d( TAG, "ServerTask: LQuerying @ " + thisPort );
							String strReceiveSplit[] = msgReceived.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							HashMap<String, String> returnValue = new HashMap<String, String>();
							try {
								Cursor resultCursor = getContext().getContentResolver().query(providerUri, null, strReceiveSplit[1], sendSelectArgs, null);
								if (resultCursor != null) {
									int valueIndex = resultCursor.getColumnIndex(DynamoConstants.VALUE_FIELD);
									if (valueIndex != -1) {
										resultCursor.moveToFirst();
										returnValue.put(strReceiveSplit[1], resultCursor.getString(valueIndex));
										Log.d(TAG, "ServerTask: The value returned is : " + returnValue.get(strReceiveSplit[1]));
									}
									resultCursor.close();
								}
							}catch( Exception e ){}
							writer.writeObject(returnValue);
						} else if(msgReceived.contains(DynamoConstants.STR_INSERT)){
							Log.d(TAG, "ServerTask: Inserting @ " + thisPort);
							msgReceived = msgReceived.replace(DynamoConstants.STR_INSERT,"");
							StringTokenizer tokenizer = new StringTokenizer(msgReceived,"%%");
							Date date = new Date();
							insertContent( tokenizer.nextToken(), date.getTime() + "##" + tokenizer.nextToken() );

							writer.writeObject(dummyMap);
						}
						writer.close();
					}
					reader.close();
				}
			}catch (IOException ioe){
				Log.e(TAG, "ServerTask socket IOException" );
				ioe.printStackTrace();
			}catch(Exception e){
				Log.e(TAG, "ServerTask Exception Unknown"+ e.getMessage());
			}
			return null;
		}

		protected void onProgressUpdate(String...strings) {

		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String getPort()
	{
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return String.valueOf((Integer.parseInt(portStr) * 2));
	}

	private int writeToFile() {

		int returnValue = 0;
		try{
			File file = getContext().getFilesDir();
			File [] all_files = file.listFiles();

			for (File del_file : all_files) {
				getContext().deleteFile(del_file.getName());
				returnValue++;
			}

		} catch (Exception e) {
			Log.e("DELETE", "Deleting files from avd failed");
		}
		return returnValue;
	}

	public void insertContent(String filename, String content){

		FileOutputStream outputStream;
		try {
			outputStream = getContext().openFileOutput( filename, Context.MODE_PRIVATE );
			outputStream.write(content.getBytes());
			outputStream.close();
			Log.d(TAG,"InsertContent: Insert new value : "+ content);
		}catch(Exception e){
			Log.e(TAG, "InsertContent: Exception" );
		}
	}

	private void fetchAllFiles() {

		String line;
		FileInputStream fis;
		BufferedReader reader;
		try{
			Log.d(TAG, "FetchAllFiles: Fetching now");
			for (String file : getContext().fileList()) {
				fis = getContext().openFileInput(file);
				reader = new BufferedReader(new InputStreamReader(fis));

				while ((line = reader.readLine()) != null) {
					queryHelper.put(file, line);
				}
			}
		}catch( Exception e ){
			Log.e(TAG, "FetchAllFiles: Exception");
		}
	}
}


