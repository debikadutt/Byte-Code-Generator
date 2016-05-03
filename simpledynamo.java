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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

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

	public static boolean waitFlag = false;
	public static boolean createFlag = false;
	public static boolean insertFlag = false;

	public static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	//private final Uri myUri = ProviderHelper.buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	public static String[] cols = new String[] {KEY_MAP, VALUE_MAP};

	/*public static ConcurrentHashMap<String, HashMap<String,String>> invalidMap = new ConcurrentHashMap<String, HashMap<String,String>>();
	public static ConcurrentHashMap<String, String> nodeRecovery = new ConcurrentHashMap<String, String>();

	public static ConcurrentHashMap<String, String> nodeStage = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, String> queryHelper = new ConcurrentHashMap<String, String>();
	public static ConcurrentSkipListMap<String, String> myPort = new ConcurrentSkipListMap<String, String>();
*/
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
						if (!nodeStage.get(node_Counter).equals(NODE_KILLED))
						{
							if (!node_Counter.equals(getPort()))
							{
								ClientTask client_task = new ClientTask();
								client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_NODE + "%%" + selection + "%#%", node_Counter);
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


	private int writeToFile() {

		int returnValue = 0;
		try{
			File file = getContext().getFilesDir();
			File [] all_files = file.listFiles();

			for(File del_file : all_files) {
				getContext().deleteFile(del_file.getName());
				returnValue++;
			}

		} catch (Exception e) {
			Log.e("DELETE", "Deleting files from avd failed");
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
		String key_name = null;
		String key_hash = null;
		String value = null;
		int countPriority = 0;

		try {
			for(String key: values.keySet())
			{
				if(key.contains("key"))
				{
					key_name = (String)values.get(key);
					key_hash = genHash(key_name);
					Log.d(TAG, "Inserted key is:" + key_name + "with hash_id" + key_hash);
				}
				else if(key.contains("value"))
				{
					value = (String)values.get(key);
					Log.d(TAG, "Inserted value is:" + value);
				}
			}

			String coordinator = ProviderHelper.findNeighbor(key_hash);
			List<String> nodePriority = ProviderHelper.getPriorityQueue(coordinator, true);
			if (nodePriority.contains(getPort()))
			{
				Date date = new Date();
				insertKeyValuePair(key_name, date.getTime() + "##" + value);
				nodePriority.remove(getPort());
				Log.d(TAG, "Insert: Final size of priority queue: " + nodePriority.size());
			}
			for(String priorElements : nodePriority)
			{
				if(isDead(priorElements)==false)
				{
					countPriority+=1;
				}
			}
			for(String priority : nodePriority)
			{
				if(isDead(priority)==false)
				{
					Log.d(TAG, "Insert: inserting " + key_name + "with priority" + priority);
					ClientTask client_task = new ClientTask();
					client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_NODE + key_name + "%%" + value + "%%" + countPriority, priority);
					countPriority--;
				}
				else
				{
					Date date = new Date();
					if (invalidMap.containsKey(priority)==false)
					{
						HashMap<String, String> hashMap = new HashMap();
						hashMap.put(key_name, date.getTime() + "##" + value);
						Log.d(TAG, "Inserted:" + key_name + "value" + value);
						invalidMap.put(priority, hashMap);
					}
					else
					{
						invalidMap.get(priority).put(key_name, date.getTime() + "##" + value);
						Log.d(TAG, "Insert missed:" + key_name + "value" + value);
					}
				}
			}
		} catch (Exception e)
		{
			Log.e( TAG, "Insert: Failure due to : " + e.getMessage());
			e.printStackTrace();
		}
		return null;
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
							myPort.put(genHash(thisPort), input);
							Log.d(TAG, "OnCreate: my port" + thisPort + "with id: " + input);
							nodeStage.put(input, NODE_ALIVE);
							if (input.equals(getPort())==false)
							{
								myCounter--;
								Log.d(TAG, "Creating ClientTask");
								ClientTask client_task = new ClientTask();
								client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, NODE_ALIVE_STATUS + "%%" + getPort() + "%%" + Integer.toString(myCounter), input);
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
					for (Map.Entry<String, String> entry : nodeRecovery.entrySet())
					{
						insertKeyValuePair(entry.getKey(), entry.getValue());
					}
					nodeRecovery = new ConcurrentHashMap();
					Log.d(TAG, "OnCreate: NodeRecovery size: " + nodeRecovery.size());
				}
			};
			newThread.start();
		} catch (Exception e){
			Log.e(TAG, "onCreate:Exception in SimpleDynamoProvider " + e.getMessage());
		}
		return false;
	}

	@Override
	/*public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
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
						fileInputOutput();
						for (String port : nodeStage.keySet())
						{
							if (nodeStage.get(port).equals(NODE_ALIVE))
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
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
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
						List<String> prefList = getPriorityQueue(findNeighbor(genHash(selection)), false);
						Log.d(TAG, "Query: Retrieving files from Queue");
						Log.d(TAG, "Query: Number of files are " + prefList.size());
						int prefSize = prefList.size();
						for (String port : prefList)
						{
							prefSize--;
							ClientTask client_task = new ClientTask();
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
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
	}*/

	/*public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder)
	{
		String read_line;
		MatrixCursor cursor = new MatrixCursor(new String[] {"key", "value"});
		String result = null;
		FileInputStream inputStream;
		BufferedReader bufferedReader;
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
						fileInputOutput();
						for (String port : nodeStage.keySet())
						{
							if (nodeStage.get(port).equals(NODE_ALIVE))
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
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}
						Log.d(TAG, "Query: waiting for messages with queue size" + prefSize);
						waitFlag = false;
						while (!waitFlag) {
						}
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
						List<String> prefList = getPriorityQueue(findNeighbor(genHash(selection)), false);
						Log.d(TAG, "Query: Retrieving files from Queue");
						Log.d(TAG, "Query: Number of files are " + prefList.size());
						int prefSize = prefList.size();
						for (String port : prefList)
						{
							prefSize--;
							ClientTask client_task = new ClientTask();
							client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}
						waitFlag = false;
						while (!waitFlag)
						{
							//do nothing
						}
						waitFlag = false;
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
	}*/

	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		String line;
		MatrixCursor cursor = new MatrixCursor(cols);
		String value = null;
		FileInputStream fis;
		BufferedReader reader;
		Log.d(TAG, "The selection parameter is : "+ selection);

		try {
			if( selection.contains("*") ){
				if(selectionArgs == null) {
					synchronized ( this ) {
						fileInputOutput();
						List<String> aliveNodes = new ArrayList<String>();
						for (String port : nodeStage.keySet()) {
							if (nodeStage.get(port).equals(DynamoConstants.NODE_ALIVE)) {
								aliveNodes.add(port);
							}
						}
						aliveNodes.remove( getPort() );
						Log.d(TAG, "Number of nodes alive is " + aliveNodes.size());
						int prefSize = aliveNodes.size();
						for (String port : aliveNodes) {
							prefSize = prefSize - 1;
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY + selection + "%%" + String.valueOf(prefSize), port);
						}
						waitFlag = false;
						while (!waitFlag) {

						}
						Log.d(TAG, "Global Query: Wait is over");
						waitFlag = false;
						for (String key : queryHelper.keySet()) {
							String values[] = queryHelper.get(key).split("##");
							values[0] = key;
							cursor.addRow(values);
						}
						queryHelper = new ConcurrentHashMap();
					}
				} else {
					Log.d(TAG, "Replying the initiator ");
					for (String file : getContext().fileList()) {
						fis = getContext().openFileInput(file);
						reader = new BufferedReader(new InputStreamReader(fis));

						while ((line = reader.readLine()) != null) {
							value = line;
						}
						String[] values = new String[]{file, value};
						cursor.addRow(values);
					}
				}
				value = null;
				Log.d( TAG, "Number of rows returned is : " + cursor.getCount() );
				return cursor;
			} else if( selection.matches( "@" ) ) {

				Log.d(TAG, "Replying the initiator ");
				for (String file : getContext().fileList()) {
					fis = getContext().openFileInput(file);
					reader = new BufferedReader(new InputStreamReader(fis));

					while ((line = reader.readLine()) != null) {
						String[] tempSplit = line.split("##");
						value = tempSplit[1];
					}
					String[] values = new String[]{file, value};
					cursor.addRow(values);
				}
				value = null;
				Log.d( TAG, "Number of rows returned is : " + cursor.getCount() );
				return cursor;
			} else {
				if(selectionArgs == null) {
					synchronized ( this ) {
						List<String> prefList = ProviderHelper.getPriorityQueue(ProviderHelper.findNeighbor(genHash(selection)), false);
						Log.d(TAG, "Fetching the replicas from the preference list");
						int prefSize = prefList.size();
						for (String port : prefList) {
							prefSize = prefSize - 1;
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DynamoConstants.LOCAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}
						waitFlag = false;
						while (!waitFlag) {

						}
						Log.d(TAG, "Local Query: Wait is over");
						waitFlag = false;
						if (queryHelper.get(selection) != null) {
							String tempSplit[] = queryHelper.get(selection).split("##");
							value = tempSplit[1];
						}
						queryHelper = new ConcurrentHashMap();
						if( value != null ) {
							String[] values = new String[]{selection, value};
							cursor.addRow(values);
						}
						Log.d(TAG, "Query: Value returned returned is : " + value);
					}
				} else {
					Log.d(TAG, "Query: Replying the initiator ");
					try {
						fis = getContext().openFileInput(selection);
						reader = new BufferedReader(new InputStreamReader(fis));
						while ((line = reader.readLine()) != null) {
							value = line;
						}
					} catch (Exception e) {
						Log.e(TAG, "Query: File does not exist");
					}
					if( value != null ) {
						String[] values = new String[]{selection, value};
						cursor.addRow(values);
					}
				}
				value = null;
				return cursor;
			}
		} catch (Exception e) {
			Log.e(TAG, "Query Exception" + e.getMessage());
			e.printStackTrace();
		}
		return cursor;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		private Uri myUri;

		public Uri buildUri(String scheme, String authority) {
			Uri.Builder uriBuilder = new Uri.Builder();
			uriBuilder.authority(authority);
			uriBuilder.scheme(scheme);
			return uriBuilder.build();
		}

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			HashMap<String,String> dummyMap = new HashMap();
			dummyMap.put(KEY_MAP, VALUE_MAP);
			String thisPort = getPort();
			String msgToRecv;
			Socket socket;
			//ObjectInputStream reader;
			//ObjectOutputStream writer;

			myUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

			try {
				while( true ) {
					socket = serverSocket.accept();

					ObjectInputStream reader = new ObjectInputStream(socket.getInputStream());
					if ((msgToRecv = (String)reader.readObject()) != null) {

						msgToRecv = msgToRecv.trim();
						ObjectOutputStream writer = new ObjectOutputStream( socket.getOutputStream() );

						if( msgToRecv.contains( NODE_ALIVE_STATUS ) ) {
							String strReceiveSplit[] = msgToRecv.split("%%");
							Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Alive" );
							if( nodeStage.containsKey( strReceiveSplit[1] ) ) {
								nodeStage.put(strReceiveSplit[1], NODE_ALIVE);
							}
							writer.writeObject(invalidMap.get(strReceiveSplit[1]));
							invalidMap.remove(strReceiveSplit[1]);
							writer.close();

						} else if( msgToRecv.contains( NODE_KILLED_STATUS ) ) {
							String strReceiveSplit[] = msgToRecv.split("%%");
							Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Dead" );
							if (nodeStage.containsKey(strReceiveSplit[1] ) ) {
								nodeStage.put(strReceiveSplit[1], NODE_KILLED);
							}
							writer.writeObject(dummyMap);

						} else if(msgToRecv.contains(DynamoConstants.DELETE_NODE)){
							String strReceiveSplit[] = msgToRecv.split("%%");
							Log.d( TAG, "ServerTask: Deleting selection : " + strReceiveSplit[1] );
							getContext().getContentResolver().delete(myUri, strReceiveSplit[1], strReceiveSplit);
							writer.writeObject(dummyMap);

						} else if(msgToRecv.contains(DynamoConstants.GLOBAL_STAR_QUERY) ){
							String strReceiveSplit[] = msgToRecv.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							Cursor resultCursor = getContext().getContentResolver().query(myUri, null, strReceiveSplit[1], sendSelectArgs, null);
							HashMap<String, String> returnValue = new HashMap<String, String>();
							try {
								if (resultCursor != null) {
									Log.d(TAG, "ServerTask: GQuerying @ " + thisPort + " returned " + resultCursor.getCount());
									int keyIndex = resultCursor.getColumnIndex(KEY_MAP);
									int valueIndex = resultCursor.getColumnIndex(VALUE_MAP);

									for (resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
										returnValue.put(resultCursor.getString(keyIndex), resultCursor.getString(valueIndex));
									}
									resultCursor.close();
								}
							}catch( Exception e ){}
							writer.writeObject(returnValue);
						} else if(msgToRecv.contains(DynamoConstants.LOCAL_STAR_QUERY)){
							Log.d( TAG, "ServerTask: LQuerying @ " + thisPort );
							String strReceiveSplit[] = msgToRecv.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							HashMap<String, String> returnValue = new HashMap<String, String>();
							try {
								Cursor resultCursor = getContext().getContentResolver().query(myUri, null, strReceiveSplit[1], sendSelectArgs, null);
								if (resultCursor != null) {
									int valueIndex = resultCursor.getColumnIndex(VALUE_MAP);
									if (valueIndex != -1) {
										resultCursor.moveToFirst();
										returnValue.put(strReceiveSplit[1], resultCursor.getString(valueIndex));
										Log.d(TAG, "ServerTask: The value returned is : " + returnValue.get(strReceiveSplit[1]));
									}
									resultCursor.close();
								}
							}catch( Exception e ){}
							writer.writeObject(returnValue);
						} else if(msgToRecv.contains(DynamoConstants.INSERT_NODE)){
							Log.d(TAG, "ServerTask: Inserting @ " + thisPort);
							msgToRecv = msgToRecv.replace(INSERT_NODE,"");
							StringTokenizer tokenizer = new StringTokenizer(msgToRecv,"%%");
							Date date = new Date();
							insertKeyValuePair( tokenizer.nextToken(), date.getTime() + "##" + tokenizer.nextToken() );

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

	/*public String findNeighbor(String input)
	{
		String coordinator = null;
		try {
			for (String key : myPort.keySet())
			{
				if (input.compareTo(key) < 0)
				{
					coordinator = myPort.get(key);
					break;
				}
				if (coordinator == null)
				{
					coordinator = myPort.firstEntry().getValue();
				}
			}
		} catch (Exception e) {
			Log.e(TAG, "FindNeighbor: Error is this " + e.getMessage());
			e.printStackTrace();
		}
		return coordinator;
	}*/

	private String getPort()
	{
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		return String.valueOf((Integer.parseInt(portStr) * 2));
	}

	public void insertKeyValuePair(String key_name, String value)
	{
		try
		{
			FileOutputStream output = getContext().openFileOutput(key_name, Context.MODE_PRIVATE);
			output.write(value.getBytes());
			output.close();
			Log.d(TAG,"insertKeyValuePair: "+ key_name + " " + value);
		} catch(IOException e){
			Log.e(TAG, "IOException" + e.getMessage());
		} catch(Exception e){
			Log.e(TAG, "insertKeyValuePair: Error in inserting key_value pair" + e.getMessage());
			e.printStackTrace();
		}
	}

	/*public List<String> getPriorityQueue(String input, boolean deadFlag)
	{
		List<String> value = new ArrayList<String>();
		try
		{
			input = genHash(String.valueOf(Integer.valueOf(input) / 2));
			int myCount = 3;

			for (Map.Entry<String, String> entry : myPort.entrySet()) {
				if (input.compareTo(entry.getKey()) <= 0)
				{
					if (deadFlag || !isDead(entry.getValue()))
					{
						value.add(entry.getValue());
					}
					myCount--;
					if (myCount == 0)
						break;
				}
			}

			if (myCount != 0)
			{
				for (Map.Entry<String, String> entry : myPort.entrySet())
				{
					if (deadFlag || !isDead(entry.getValue()))
					{
						value.add(entry.getValue());
					}
					myCount--;
					if (myCount == 0)
						break;
				}
			}

		} catch (Exception e) {
			Log.e(TAG, "getPriorityQueue: Exception due to = " + e.getMessage());
			e.printStackTrace();
		}
		return value;
	}*/

	public void fileInputOutput()
	{
		String readLine;
		try
		{
			Log.d(TAG, "fileInputOutput: " + getContext().fileList());
			for (String file : getContext().fileList())
			{
				FileInputStream input = getContext().openFileInput(file);
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(input));
				readLine = bufferedReader.readLine();
				while (readLine != null)
				{
					queryHelper.put(file, readLine);
				}
			}
		} catch(IOException e){
			Log.e(TAG, "fileInputOutput: Error in i/o operation" + e.getMessage());
		} catch (Exception e){
			Log.e(TAG, "fileInputOutput: Error" + e.getMessage());
			e.printStackTrace();
		}
	}

	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public static boolean isDead(String input) {
		Log.d(TAG, "isDead: " + nodeStage.values());
		boolean return_value = nodeStage.get(input).equals(NODE_KILLED);
		return return_value;
	}
}
