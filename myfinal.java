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


public class SimpleDynamoProvider extends ContentProvider {

	public static final int SERVER_PORT = 10000;

	public static final String NODE_DELETE = "delete";
	public static final String GLOBAL_STAR_QUERY = "Global_query";
	public static final String LOCAL_STAR_QUERY = "Local_query";
	public static final String SIMPLE_QUERY = "query_node";
	public static final String INSERT_NODE = "insert_node";
	public static final String NODE_LIVE = "alive_node";
	public static final String NODE_DEAD = "dead_node";
	public static final String REPORT_NODE_DEAD ="report_dead_node";
	public static final String REPORT_NODE_ALIVE ="report_alive_node";
	public static boolean flag_to_recover = false;
	public static boolean flag_to_create = false;
	public static boolean flag_to_insert = false;
	public static boolean flag_to_delete = false;

	public static final String GET_KEY = "key";
	public static final String GET_VALUE = "value";

	public static ConcurrentHashMap<String, HashMap<String,String>> killed_avd_map = new ConcurrentHashMap<String, HashMap<String,String>>();
	public static ConcurrentHashMap<String, String> get_missed_msgs = new ConcurrentHashMap<String, String>();

	public static List<String> REMOTE_PORTS = new ArrayList<String>(){{
		add("11108");add("11112");add("11116");add("11120");add("11124");
	}};


	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	private final Uri providerUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private static String[] cols = new String[] {GET_KEY, GET_VALUE};

	public static ConcurrentHashMap<String, String> getPortState = new ConcurrentHashMap<String, String>();
	public static ConcurrentHashMap<String, String> getQueries = new ConcurrentHashMap<String, String>();
	public static ConcurrentSkipListMap<String, String> getPortMap = new ConcurrentSkipListMap<String, String>();



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		int returnValue = 0;
		Log.d(TAG, "Inside delete");
		try {
			if (selection.matches("\"*\""))
			{
				Log.d(TAG, "Selection is *");
				returnValue = writeToFile();
				if(selectionArgs == null)
				{
					//for(int i = 11108; i <= REMOTE_PORTS.size(); i+=4)
					for (String node_Counter : REMOTE_PORTS)
					{
						Log.d(TAG, "Port state:" + getPortState.get(node_Counter));
						if (!getPortState.get(node_Counter).equals(NODE_DEAD))
						{
							if (!node_Counter.equals(getPort()))
							{
								ClientTask client_task = new ClientTask();
								client_task.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, NODE_DELETE + "%%" + selection + "%#%", node_Counter);
							}
						}
					}
				}
			}
			else if(selection.matches("\"@\""))
			{
				Log.d(TAG, "Selection is @");
				returnValue = writeToFile();
				Log.d(TAG, "Deleting all data from current avd" + returnValue);
			}
			else
			{
				writeToFile();
			}
			Log.e(TAG, "Deleting all data from current avd" + returnValue);
		}catch(Exception e){
			Log.e(TAG, "Delete: Failed" + e.getMessage());
		}
		return returnValue;
	}

	private int writeToFile() 
	{

		int returnValue = 0;
		try{
			File file = getContext().getFilesDir();
			File [] all_files = file.listFiles();

			for (File del_file : all_files) {
				getContext().deleteFile(del_file.getName());
				returnValue++;
			}

		} catch (Exception e) {
			Log.e("Delete: ", "Deleting files from avd failed");
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

		String getPort_value = getPort();
		try {
			for(String key: values.keySet())
			{

				if(key.equals("key"))
				{
					Log.d(TAG, "Insert: key is" + key.equals("key"));
					key_name = String.valueOf(values.get(key));
					key_hash = genHash(key_name);
				}
				else
				{
					Log.d(TAG,"Insert otherwise: ");
					value = String.valueOf(values.get(key));
				}
			}
			Log.d(TAG, "Inserted: " + key_name);

			List<String> myQueue = getPriorityQueue(ProviderHelper.findNeighbor(key_hash), true);

			if (myQueue.contains(getPort_value))
			{
				Log.d(TAG, "Insert: value in myQueue " + myQueue.contains(getPort_value));
				Date date = new Date();
				insertHelper(key_name, date.getTime() + "##" + value);
				myQueue.remove(getPort_value);
			}
			int myCounter = 0;

			for(String s : myQueue)
			{
				Log.d(TAG, "Insert: checking if dead");
				if(!isDead(s))
				{
					myCounter ++;
				}
			}
			for (String port_name : myQueue)
			{
				if (!isDead(port_name))
				{
					Log.d(TAG, "Insert: inserting " + key_name);

					myCounter--;

					Log.d(TAG, "Creating ClientTask");
					//ClientTask client_task = new ClientTask();
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_NODE + key_name + "%%" + value + "%%" + myCounter, port_name);
				}
				else
				{
					Log.d(TAG, "Insert: missed : " + port_name + " " + key_name);

					Date date = new Date();

					if (!killed_avd_map.containsKey(port_name))
					{
						Log.d(TAG, "Insert: put in temp map");
						HashMap<String, String> myMap = new HashMap();
						myMap.put(key_name, date.getTime() + "##" + value);
						killed_avd_map.put(port_name, myMap);
					}
					else
					{
						Log.d(TAG, "Insert: put in killed_avd_map");
						killed_avd_map.get(port_name).put(key_name, date.getTime() + "##" + value);
					}
				}
			}

			// check flag
			flag_to_insert = false;
			while (!flag_to_insert)
			{
				//infinite loop
			}
			flag_to_insert = false;

		} catch ( Exception e )
		{
			Log.e( TAG, "Insert: Failed");
			e.getMessage();
			e.printStackTrace();
		}

		return uri;
	}

	@Override
	public boolean onCreate() {

		try {
			try
			{
				Log.d(TAG, "Creating a ServerSocket in Oncreate");

				ServerSocket server_socket = new ServerSocket(SERVER_PORT);
				new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, server_socket);
			}
			catch (IOException e)
			{
				Log.e(TAG, "onCreate: ServerSocket failed to create");
				return false;
			}
			final String myPort = getPort();

			Thread createThread = new Thread()
			{
				@Override
				public void run() {

					int iCount = 4;
					for (String port : REMOTE_PORTS)
					{
						try {

							//
							getPortMap.put(genHash(String.valueOf(Integer.valueOf(port) / 2)), port);

							getPortState.put(port, NODE_LIVE);
							if (!port.equals(myPort)) {
								iCount = iCount - 1;
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, REPORT_NODE_ALIVE + "%%" + myPort + "%%" + String.valueOf(iCount), port);
							}
						} catch (Exception e) {
							Log.e(TAG, "OnCreate: Exception while reporting aliveness " + e.getMessage());
						}
					}
					flag_to_create = false;
					while (!flag_to_create)
					{
						//infinite loop
					}
					flag_to_create = false;

					Log.d(TAG, "onCreate: Recoverd msgs" + get_missed_msgs.size());

					for (Map.Entry<String, String> entry : get_missed_msgs.entrySet())
					{
						insertHelper(entry.getKey(), entry.getValue());
					}
					get_missed_msgs = new ConcurrentHashMap();
				}
			};
			Log.d(TAG, "Start new thread");
			createThread.start();
		}catch(Exception e){
			Log.e(TAG, "onCreate:Exception " + e.getMessage());
		}
		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

		String read_line;
		//String myport = getPort();
		//String avd = String.valueOf(Integer.parseInt(myport) / 2);
		MatrixCursor cursor = new MatrixCursor(cols);
		String value = null;
		FileInputStream input;
		BufferedReader bufferedReader;

		try {
			if(selection.contains("*"))
			{
				Log.d(TAG, "Selection is * ");
				if(selectionArgs == null) 
				{
					synchronized(this) 
					{
						getReplicas();
						List<String> aliveNodes = new ArrayList<String>();
						for (String s : getPortState.keySet())
						{
							Log.d(TAG, "Query: Get port state" + getPortState.get(s));
							if (getPortState.get(s).equals(NODE_LIVE))
							{
								aliveNodes.add(s);
							}
						}
						aliveNodes.remove(getPort());

						int prioritySize = aliveNodes.size();
						Log.d(TAG, "# Alive node count: " + prioritySize);
						for (String port : aliveNodes)
						{
							prioritySize = prioritySize - 1;
							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, GLOBAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prioritySize), port);
						}
						flag_to_recover = false;
						while (!flag_to_recover) {

						}
						Log.d(TAG, "Global querying over..");
						flag_to_recover = false;
						for (String key : getQueries.keySet()) {
							String values[] = getQueries.get(key).split("##");
							values[0] = key;
							cursor.addRow(values);
						}
						getQueries = new ConcurrentHashMap();
					}
				} else
				{
					for (String file : getContext().fileList())
					{
						input = getContext().openFileInput(file);
						bufferedReader = new BufferedReader(new InputStreamReader(input));

						while ((read_line = bufferedReader.readLine()) != null)
						{
							value = read_line;
						}
						String[] values = new String[]{file, value.toString()};
						cursor.addRow(values);
					}
				}
				value = null;
				Log.d("Count", cursor.getCount() + "");
				return cursor;

			}
			else if(selection.matches("@"))
			{
				for (String file : getContext().fileList())
				{
					input = getContext().openFileInput(file);
					bufferedReader = new BufferedReader(new InputStreamReader(input));

					while ((read_line = bufferedReader.readLine()) != null)
					{
						String[] tempSplit = read_line.split("##");
						value = tempSplit[1];
					}
					String[] values = new String[]{file, value};
					cursor.addRow(values);
					Log.d("Count", cursor.getCount() + "");
				}
				value = null;

				return cursor;
			} else
			{
				if(selectionArgs == null)
				{
					synchronized (this)
					{
						List<String> prefList = getPriorityQueue(ProviderHelper.findNeighbor(genHash(selection)), false);

						int prefSize = prefList.size();
						for (String port : prefList)
						{
							Log.d(TAG, "Query: myQueue size" + prefSize);
							prefSize = prefSize - 1;
							ClientTask clientTask = new ClientTask();
							clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, LOCAL_STAR_QUERY + "%%" + selection + "%%" + String.valueOf(prefSize), port);
						}

						flag_to_recover = false;
						while (!flag_to_recover)
						{
							//infinite loop
						}
						Log.d(TAG, "Local Querying over..");
						flag_to_recover = false;

						if (getQueries.get(selection) != null)
						{
							String tempSplit[] = getQueries.get(selection).split("##");
							value = tempSplit[1];
						}
						getQueries = new ConcurrentHashMap();
						if( value != null )
						{
							String[] values = new String[]{selection, value};
							cursor.addRow(values);
						}
						Log.d(TAG, "Query: Value : " + value);
					}
				} else {

					try
					{
						input = getContext().openFileInput(selection);
						bufferedReader = new BufferedReader(new InputStreamReader(input));
						while ((read_line = bufferedReader.readLine()) != null)
						{
							value = read_line;
						}

					} catch (Exception e)
					{
						Log.e(TAG, "Query: Unable to open file");
					}
					if( value != null )
					{
						String[] values = new String[]{selection, value};
						cursor.addRow(values);
					}
				}
				value = null;
				return cursor;
			}
		} catch (Exception e) {
			Log.e(TAG, "Query Failed" + e.getMessage());
			e.printStackTrace();
		}
		return cursor;
	}



	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			HashMap<String,String> dummyMap = new HashMap();
			dummyMap.put(GET_KEY, GET_VALUE);
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

						if( msgReceived.contains(REPORT_NODE_ALIVE) ) {
							String strReceiveSplit[] = msgReceived.split("%%");
							Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Alive" );
							if( getPortState.containsKey( strReceiveSplit[1] ) ) {
								getPortState.put(strReceiveSplit[1], NODE_LIVE);
							}
							writer.writeObject(killed_avd_map.get(strReceiveSplit[1]));
							killed_avd_map.remove(strReceiveSplit[1]);
							writer.close();

						} else if( msgReceived.contains(REPORT_NODE_DEAD) ) {
							String strReceiveSplit[] = msgReceived.split("%%");
							Log.d( TAG, "ServerTask: "+strReceiveSplit[1]+" is reported Dead" );
							if( getPortState.containsKey( strReceiveSplit[1] ) ) {
								getPortState.put(strReceiveSplit[1], NODE_DEAD);
							}
							writer.writeObject(dummyMap);

						} else if(msgReceived.contains(NODE_DELETE)){
							String strReceiveSplit[] = msgReceived.split("%%");
							Log.d( TAG, "ServerTask: Deleting selection : " + strReceiveSplit[1] );
							getContext().getContentResolver().delete(providerUri, strReceiveSplit[1], strReceiveSplit);
							writer.writeObject(dummyMap);

						} else if(msgReceived.contains(GLOBAL_STAR_QUERY) ){
							String strReceiveSplit[] = msgReceived.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							Cursor resultCursor = getContext().getContentResolver().query(providerUri, null, strReceiveSplit[1], sendSelectArgs, null);
							HashMap<String, String> returnValue = new HashMap<String, String>();
							try {
								if (resultCursor != null) {
									Log.d(TAG, "ServerTask: GQuerying @ " + thisPort + " returned " + resultCursor.getCount());
									int keyIndex = resultCursor.getColumnIndex(GET_KEY);
									int valueIndex = resultCursor.getColumnIndex(GET_VALUE);

									for (resultCursor.moveToFirst(); !resultCursor.isAfterLast(); resultCursor.moveToNext()) {
										returnValue.put(resultCursor.getString(keyIndex), resultCursor.getString(valueIndex));
									}
									resultCursor.close();
								}
							}catch( Exception e ){}
							writer.writeObject(returnValue);
						} else if(msgReceived.contains(LOCAL_STAR_QUERY)){
							Log.d( TAG, "ServerTask: LQuerying @ " + thisPort );
							String strReceiveSplit[] = msgReceived.split("%%");
							String sendSelectArgs[] = new String[]{strReceiveSplit[1]};
							HashMap<String, String> returnValue = new HashMap<String, String>();
							try {
								Cursor resultCursor = getContext().getContentResolver().query(providerUri, null, strReceiveSplit[1], sendSelectArgs, null);
								if (resultCursor != null) {
									int valueIndex = resultCursor.getColumnIndex(GET_VALUE);
									if (valueIndex != -1) {
										resultCursor.moveToFirst();
										returnValue.put(strReceiveSplit[1], resultCursor.getString(valueIndex));
										Log.d(TAG, "ServerTask: The value returned is : " + returnValue.get(strReceiveSplit[1]));
									}
									resultCursor.close();
								}
							}catch( Exception e ){}
							writer.writeObject(returnValue);
						} else if(msgReceived.contains(INSERT_NODE)){
							Log.d(TAG, "ServerTask: Inserting @ " + thisPort);
							msgReceived = msgReceived.replace(INSERT_NODE,"");
							StringTokenizer tokenizer = new StringTokenizer(msgReceived,"%%");
							Date date = new Date();
							insertHelper(tokenizer.nextToken(), date.getTime() + "##" + tokenizer.nextToken());

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



	public void insertHelper(String key_name, String value)
	{

		try
		{
			FileOutputStream output = getContext().openFileOutput(key_name, Context.MODE_PRIVATE);
			output.write(value.getBytes());
			output.close();
			Log.d(TAG,"insertHelper: Inserting value at: "+ getPort()+key_name + " " + value);
		}catch(IOException e){
			Log.e(TAG, "insertHelper: Unable to open file");
		}
		catch(Exception e){
			Log.e(TAG, "insertHelper: other exception" + e.getMessage());
			e.printStackTrace();
		}
	}

	private void getReplicas()
	{

		String read_line;

		BufferedReader bufferedReader;
		try{
			Log.d(TAG, "getReplicas: getting replica now");
			for (String file : getContext().fileList())
			{
				FileInputStream input = getContext().openFileInput(file);
				bufferedReader = new BufferedReader(new InputStreamReader(input));

				while ((read_line = bufferedReader.readLine())!= null)
				{
					getQueries.put(file, read_line);
				}
			}
		}catch(Exception e)
		{
			Log.e(TAG, "getReplicas: Exception occurred" + e.getMessage());
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

	public static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	public static List<String> getPriorityQueue(String input, boolean deadFlag) {

		List<String> retVal = new ArrayList<String>();
		try{
			input = genHash(String.valueOf(Integer.valueOf(input) / 2));
			int countMembers = 3;

			for( Map.Entry<String, String> entry : getPortMap.entrySet() ){
				if( input.compareTo(entry.getKey()) <= 0 ) {
					if( deadFlag || !isDead( entry.getValue() ) ) {
						retVal.add(entry.getValue());
					}
					countMembers--;
					if(countMembers == 0)
						break;
				}
			}

			if( countMembers != 0 ){
				for(Map.Entry<String, String> entry : getPortMap.entrySet())
				{
					if(deadFlag || !isDead( entry.getValue()))
					{
						retVal.add(entry.getValue());
					}
					countMembers--;
					if(countMembers == 0)
						break;

				}
			}

		} catch(Exception e){
			Log.e(TAG, "GetPreferenceList: Exception message = " + e.getMessage() );
		}
		return retVal;
	}

	public static boolean isDead( String input ) {

		return getPortState.get( input ).equals( NODE_DEAD );
	}
}


