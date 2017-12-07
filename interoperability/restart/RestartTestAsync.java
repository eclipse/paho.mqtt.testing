/*
 * IBM Confidential
 * OCO Source Materials
 *  
 * 5724-H72
 *  
 * (c) Copyright IBM  Corp. 2011, 2013
 * The source code for this program is not published or other-
 * wise divested of its trade secrets, irrespective of what has
 * been deposited with the U.S. Copyright Office.
 * <END_COPYRIGHT>
 * 
 * Version: %Z% %W% %I% %E% %U%
 */

package com.ibm.mqst.mqxr.restart;

import com.ibm.mqst.log.Loga;
import org.eclipse.paho.client.mqttv3.*;

import java.util.*;
import java.lang.Thread;
import java.lang.System;

public class RestartTestAsync extends Thread implements MqttCallback {
	static HashMap<String, String> options = new HashMap<String, String>();

	volatile boolean stopping = false;
	Loga loga = null;
	String wait_message = null, wait_message2 = null;
	int control_found = 0;
	int errors = 0;
	int arrivedCount = 0, expectedCount = 0;
	MqttAsyncClient client = null;
	ControlConnection control = null;
	boolean measuring = false;
	int roundtrip_time = 0;
	long global_start_time = 0L;
	MqttConnectOptions connectOptions = null;
	int myindex = -1;
	volatile String topic = null, clientid = null;
	int last_completion_time = -1;
	boolean connected = false;
	boolean subscribed = false;
	
	IMqttActionListener clientCon = new IMqttActionListener() {
		public void onSuccess(IMqttToken aToken) {
			loga.logaLine(loga.LOGA_ALWAYS, "Client " + clientid + " connected");
			connected = true;
			IMqttActionListener clientSub = new IMqttActionListener() {
				public void onSuccess(IMqttToken aToken) {
					loga.logaLine(loga.LOGA_ALWAYS, "Client " + clientid + " subscribed");
					subscribed = true;
				}
				public void onFailure(IMqttToken aToken, Throwable exception) {
					loga.logaLine(loga.LOGA_ALWAYS, "Client " + clientid + " subscribe failed " + exception);
					//throw(exception);
				}
			};
			try {
				loga.logaLine(loga.LOGA_ALWAYS, "Subscribe options: " + topic + " " + Integer.parseInt(options.get("qos")));
				client.subscribe(topic, Integer.parseInt(options.get("qos")), null, clientSub);
			} 
			catch (MqttException ex) {
				//throw(ex);
			}
		}
		public void onFailure(IMqttToken aToken, Throwable exception) {
			loga.logaLine(loga.LOGA_ALWAYS, "Client " + clientid
					+ " connect failed" + exception);
			if (stopping)
				return;
			sleep(3);
			try {
				client.connect(connectOptions, null, clientCon);
			} catch (MqttException ex) {
				loga.logaLine(loga.LOGA_ALWAYS, "Connect call failed");
			}
		}
	};

	public RestartTestAsync(int i) {
		i++;
		myindex = i;
		int loglevel = options.get("verbose").equals("1") ? Loga.LOGA_ALWAYS
				: Loga.LOGA_INFO;
		loga = new Loga("Loga", this.getClass().getName() + "_" + myindex,
				loglevel);
	}

	static void getopts(String[] argv) {
		int count = 0;
		int argc = argv.length;

		while (count < argc) {
			if (argv[count].startsWith("--")) {
				String key = argv[count].substring(2);
				if (options.keySet().contains(key)) {
					String value = "1";
					if (count + 1 < argc && !argv[count + 1].startsWith("--"))
						value = argv[++count];
					options.put(key, value);
				} else
					usage();
			}
			count++;
		}
	}

	static void usage() {
		System.out.print("Options should be one of " + options.keySet() + "\n");
		System.exit(99);
	}

	public static void main(String[] args) {
		options.put("connection", "tcp://localhost:1884");
		options.put("control_connection", "tcp://localhost:7777");
		options.put("topic", "XR9TT3");
		options.put("controltopic", "XR9TT3/control");
		options.put("clientid", "XR9TT3_javaA");
		options.put("slot_no", "1");
		options.put("qos", "2");
		options.put("retained", "false");
		options.put("username", "");
		options.put("password", "");
		options.put("verbose", "0");
		options.put("threads", "1");
		options.put("haconnection", "");

		getopts(args);

		int no_clients = new Integer(options.get("threads"));
		RestartTestAsync[] threads = new RestartTestAsync[no_clients];

		for (int i = 0; i < no_clients; ++i) {
			threads[i] = new RestartTestAsync(i);
			threads[i].start();
		}

		boolean finished = false;
		while (finished) {
			finished = true;
			for (int i = 0; i < no_clients; ++i) {
				if (threads[i].isAlive()) {
					finished = false;
					sleep(2);
					break;
				}
			}
		}

	}

	long start_clock() {
		return System.currentTimeMillis();
	}

	int elapsed(long start) {
		return (int) (System.currentTimeMillis() - start);
	}

	static void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000L);
		} catch (Exception e) {

		}
	}

	public class ControlConnection implements MqttCallback {
		String pubTopic = null;
		MqttAsyncClient client = null;
		MqttConnectOptions connectOptions = new MqttConnectOptions();
		Boolean connected = false;
		Boolean subscribed = false;

		ControlConnection() throws Exception {
			client = new MqttAsyncClient(options.get("control_connection"), clientid + "control", null);
			client.setCallback(this);
			client.connect(connectOptions, null, new IMqttActionListener() {
				public void onSuccess(IMqttToken aToken) {
					loga.logaLine(loga.LOGA_ALWAYS, "Control connected");
					connected = true;
					try {
						client.subscribe(options.get("controltopic")+"/send", Integer.parseInt(options.get("qos")), null, new IMqttActionListener() {
						public void onSuccess(IMqttToken aToken) {
							loga.logaLine(loga.LOGA_ALWAYS, "Control subscribed");
							subscribed = true;
						}
						public void onFailure(IMqttToken aToken, Throwable exception) {
							loga.logaLine(loga.LOGA_ALWAYS, "Control subcribe failed "+exception);
							//throw(exception);
						}
					});
					} catch (MqttException ex) {
						return;
					}
				}
				public void onFailure(IMqttToken aToken, Throwable exception) {
					loga.logaLine(loga.LOGA_ALWAYS, "Control connect failed"+exception);
					
					//throw(exception);
				}
			});
			pubTopic = options.get("controltopic")+"/receive";
		}

		void finish() {
			try {
				client.disconnect(1000);
			} catch (Exception e) {

			}
		}

		public void connectionLost(java.lang.Throwable cause) {
			loga.logaLine(Loga.LOGA_ALWAYS,
					"Control connection lost - stopping");

			stopping = true;
		}

		public void deliveryComplete(IMqttDeliveryToken token) {

		}

		public void messageArrived(String topic, MqttMessage message)
				throws Exception {
			try {
				String str = new String(message.getPayload());
				loga.logaLine(Loga.LOGA_ALWAYS, "Control message arrived "
						+ str + " " + wait_message);

				if (str.equals("stop"))
					stopping = true;
				else if (wait_message != null && wait_message.equals(str)) {
					control_found = 1;
					wait_message = null;
				} else if (wait_message2 != null && wait_message2.equals(str)) {
					control_found = 2;
					wait_message2 = null;
				}
			} catch (Exception e) {

			}
		}

		/* wait for a specific message on the control topic. */
		int which(String message1, String message2) {
			int count = 0;
			control_found = 0;
			wait_message = message1;
			wait_message2 = message2;

			while (control_found == 0) {
				if (++count == 120 || stopping)
					return 0; /*
							 * time out and tell the caller the message was not
							 * found
							 */
				sleep(1);
			}
			return control_found;
		}

		void send(String msg) {
			try {
				String m = clientid + ": " + msg;
				MqttMessage ttMsg = new MqttMessage(m.getBytes());
				ttMsg.setQos(1);
				loga.logaLine(Loga.LOGA_ALWAYS, "Sending control message " + m);
				client.publish(pubTopic, ttMsg, null, new IMqttActionListener() {
					public void onSuccess(IMqttToken aToken) {
						loga.logaLine(loga.LOGA_ALWAYS, "Control message sent");
					}
					public void onFailure(IMqttToken aToken, Throwable exception) {
						loga.logaLine(loga.LOGA_ALWAYS, "Control message failed to send "+exception);
						//throw(exception);
					}
				});
			} catch (Exception e) {
				loga.logaLine(Loga.LOGA_INFO, e.getMessage());
				e.printStackTrace();
			}
		}

		/* wait for a specific message on the control topic. */
		void wait(String message) throws MqttException {
			int count = 0;

			control_found = 0;
			wait_message = message;

			send("waiting for: " + message);

			while (control_found == 0) {
				if (stopping)
					throw new MqttException(121);
				else if (++count == 240) {
					stopping = true;
					loga.logaLine(Loga.LOGA_ALWAYS,
							"Failed to receive message " + message
									+ " - stopping");
					throw new MqttException(120);
				}
				sleep(1);
			}
		}
	}

	public void run() {
		loga.logaLine(Loga.LOGA_INFO,
				"Starting restart test Java client program with options "
						+ options.toString());
		topic = options.get("topic") + "_" + options.get("slot_no") + "_"
				+ myindex;
		clientid = options.get("clientid") + "_" + options.get("slot_no") + "_"
				+ myindex;

		loga.logaLine(Loga.LOGA_INFO, "Starting with clientid " + clientid);
		loga.logaLine(Loga.LOGA_INFO, "topic is " + topic);

		try {
			control = new ControlConnection();
			while (!control.connected && !control.subscribed) {
				sleep(1);
			}
			sendAndReceive();
		} 
		catch (MqttException ex) {
		}
		catch (Exception e) {
		} 
		if (control != null)
			control.finish();
		loga.logaLine(Loga.LOGA_INFO, "Ending restart test Java client thread "
				+ myindex);
	}

	public void connectionLost(java.lang.Throwable cause) {
		loga.logaLine(Loga.LOGA_ALWAYS, "Connection lost when " + arrivedCount
				+ " messages arrived out of " + expectedCount + " expected");
		try {
			loga.logaLine(Loga.LOGA_ALWAYS, "Attempting to reconnect");
			client.connect(connectOptions, null, clientCon);
		} catch (Exception e) {
			loga.logaLine(Loga.LOGA_ALWAYS,
					"Failed to reconnect with exception " + e.getMessage());
			if (stopping)
				return;
			sleep(3);
		}
		loga.logaLine(Loga.LOGA_ALWAYS, "Successfully reconnected");
		sleep(1);
	}

	public void deliveryComplete(IMqttDeliveryToken token) {

	}

	public void messageArrived(String topic, MqttMessage message)
			throws Exception {
		String str = new String(message.getPayload());
		StringTokenizer st = new StringTokenizer(str);

		st.nextToken(); /* message */
		st.nextToken(); /* number */

		int seqno = new Integer(st.nextToken()).intValue();
		int qos = new Integer(options.get("qos")).intValue();
		if (message.getQos() != qos) {
			loga.logaLine(Loga.LOGA_ALWAYS, "Error, expecting QoS "
					+ options.get("qos") + " but got " + qos);
			errors++;
		} else if (seqno != arrivedCount + 1) {
			if (qos == 2 || (qos == 1 && seqno > arrivedCount + 1)) {
				loga.logaLine(Loga.LOGA_ALWAYS,
						"Error, expecting sequence number "
								+ (arrivedCount + 1) + " but got " + seqno);
				errors++;
			}
		}
		arrivedCount++;

		if (measuring && arrivedCount == 100)
			roundtrip_time = elapsed(global_start_time);
	}

	int sendAndReceive() throws Exception {
		int rc = 0;

		connectOptions = new MqttConnectOptions();
		connectOptions.setCleanSession(false);
		
		if (options.get("haconnection") != "")
		{
			connectOptions.setServerURIs(options.get("haconnection").split(" "));
		}
		

		loga.logaLine(Loga.LOGA_ALWAYS, "Async Java client topic workload using QoS "
				+ options.get("qos"));
		loga.logaLine(Loga.LOGA_ALWAYS, "Connecting to "
				+ options.get("connection"));

		client = new MqttAsyncClient(options.get("connection"), clientid, null);
		client.setCallback(this);

		/*
		 * wait to know that the controlling process is running before
		 * connecting to the SUT
		 */
		control.wait("who is ready?");

		client.connect(connectOptions, null, clientCon);
		
		while (!connected && !subscribed) {
			loga.logaLine(Loga.LOGA_ALWAYS, "connected: " + connected + " subscribed" + subscribed);
			sleep(1);
		}

		while (true) {
			control.send("Ready");
			if (control.which("who is ready?", "continue") == 2)
				break;
			control.send("Ready");
		}

		while (!stopping) {
			try {
				one_iteration(client);
			} catch (Exception e) {
				stopping = true;
			}
		}

		loga.logaLine(Loga.LOGA_ALWAYS,
				"Ending Java client topic workload using QoS "
						+ options.get("qos"));
		client.disconnect(10000);
		return rc;
	}

	void one_iteration(MqttAsyncClient client) throws MqttException {
		int i = 0;
		int seqno = 0;
		long start_time = 0L;
		int test_count = 100;
		int test_interval = 30;
		int last_expected_count = expectedCount;
		IMqttDeliveryToken lastMsg = null;
		
		control.wait("start_measuring");

		/* find the time for "test_count" round-trip messages */
		loga.logaLine(Loga.LOGA_ALWAYS, "Evaluating how many messages needed");
		expectedCount = arrivedCount = 0;
		global_start_time = start_clock();

		if (last_completion_time == -1) {
			loga.logaLine(Loga.LOGA_ALWAYS, "Going in to loop");
			measuring = true;
			for (i = 1; i <= test_count; ++i) {
				MqttMessage payload = new MqttMessage(("message number " + i)
						.getBytes());
				payload.setQos(new Integer(options.get("qos")));
				payload.setRetained(new Boolean(options.get("retained")));
				boolean msgSent = false;

				do {
					try {
						//loga.logaLine(Loga.LOGA_ALWAYS, "Publishing test message to topic " + topic);
						lastMsg = client.publish(topic, payload, i,
								new IMqttActionListener() {
									public void onSuccess(IMqttToken aToken) {
										/*
										 * loga.logaLine(Loga.LOGA_ALWAYS,
										 * "Test message published " + (Integer)
										 * aToken .getUserContext());
										 */
									}

									public void onFailure(IMqttToken aToken,
											Throwable exception) {
										loga.logaLine(loga.LOGA_ALWAYS, "Counting message failed to send " + exception);
									}
								}); // .waitForCompletion();
						msgSent = true;
					} catch (Exception e) {
						loga.logaLine(loga.LOGA_ALWAYS, "Client message failed to send " + e);
						if (lastMsg != null) {
							boolean waitComplete = false;
							while (waitComplete == false) {
								try {
									lastMsg.waitForCompletion();
									waitComplete = true;
								} catch (Exception f) {
									loga.logaLine(loga.LOGA_ALWAYS, "waitForCompletion exception " + f);
									sleep(1);
								}
							}
						}
						if (stopping)
							return;
					}
				} while (!msgSent);
			}
			lastMsg = null;

			loga.logaLine(Loga.LOGA_INFO, "Messages sent... waiting for echoes");
			
			while (arrivedCount < test_count) {
				if (stopping)
					return;
				sleep(1);
			}
			measuring = false;

			loga.logaLine(Loga.LOGA_ALWAYS, "Round trip time for " + test_count
					+ " messages is " + roundtrip_time + " ms");
			expectedCount = 1000 * test_count * test_interval / roundtrip_time;
		} else {
			/* Now set a target of "test_interval" seconds total round trip */
			loga.logaLine(Loga.LOGA_ALWAYS, "Last time, " + last_expected_count
					+ " messages took " + last_completion_time + " s.");
			expectedCount = last_expected_count * test_interval
					/ last_completion_time;
		}
		loga.logaLine(Loga.LOGA_ALWAYS, "Therefore " + expectedCount
				+ " messages needed for " + test_interval + " seconds");

		control.wait("start_test"); /* now synchronize the test interval */

		loga.logaLine(Loga.LOGA_ALWAYS, "Starting " + test_interval
				+ " second test run with " + expectedCount + " messages");
		arrivedCount = 0;
		start_time = start_clock();
		while (seqno < expectedCount) {
			seqno++;
			MqttMessage payload = new MqttMessage(("message number " + seqno)
					.getBytes());
			payload.setQos(new Integer(options.get("qos")));
			payload.setRetained(new Boolean(options.get("retained")));
			boolean msgSent = false;
			do {
				try {
					//loga.logaLine(Loga.LOGA_ALWAYS, "Publishing test message to topic " + topic);
					lastMsg = client.publish(topic, payload, seqno,
							new IMqttActionListener() {
								public void onSuccess(IMqttToken aToken) {
									/*loga.logaLine(Loga.LOGA_ALWAYS,
											"Test message published "
													+ (Integer) aToken
															.getUserContext());*/
									// loga.logaLine(loga.LOGA_ALWAYS,
									// "Client message sent");
								}

								public void onFailure(IMqttToken aToken,
										Throwable exception) {
									loga.logaLine(loga.LOGA_ALWAYS,
											"Test message send onFailure " + (Integer) aToken.getUserContext() 
													+ exception);
									exception.printStackTrace();
								}
							}); // .waitForCompletion();
					msgSent = true;
				} catch (Exception e) {
					loga.logaLine(loga.LOGA_ALWAYS,
							"Client message failed to send " + e);
					if (lastMsg != null) {
						boolean waitComplete = false;
						while (waitComplete == false) {
							try {
								lastMsg.waitForCompletion();
								waitComplete = true;
							} catch (Exception f) {
								loga.logaLine(loga.LOGA_ALWAYS, "waitForCompletion exception " + f);
								sleep(1);
							}
						}
					}
					if (stopping)
						return;
				}
			} while (!msgSent);
			/*try {
				loga.logaLine(Loga.LOGA_DEBUG, "Publishing message " + seqno);
				client.publish(topic, payload, seqno, new IMqttActionListener() {
					public void onSuccess(IMqttToken aToken) {
						loga.logaLine(Loga.LOGA_ALWAYS, "Message " + (Integer) aToken.getUserContext() + " published");
					}

					public void onFailure(IMqttToken aToken, Throwable exception) {
						loga.logaLine(loga.LOGA_ALWAYS, "Client message failed to send " + exception);
					}
				}); //.waitForCompletion();
			} catch (Exception e) {
				if (stopping) {
					return;
				}
				Thread.sleep(5);
			}*/
		}

		loga.logaLine(Loga.LOGA_ALWAYS, expectedCount + " messages sent in "
				+ elapsed(start_time) / 1000 + " seconds");

		waitForCompletion(start_time, expectedCount);
		control.wait("test finished");
	}

	int waitForCompletion(long start_time, int expectedCount) {
		int lastreport = 0;
		int wait_count = 0;
		int limit = 120;

		sleep(1);
		while (arrivedCount < expectedCount) {
			if (arrivedCount > lastreport) {
				loga.logaLine(Loga.LOGA_ALWAYS, arrivedCount
						+ " messages arrived out of " + expectedCount
						+ " expected, in " + elapsed(start_time) / 1000
						+ " seconds");
				lastreport = arrivedCount;
			}
			sleep(1);
			if (++wait_count > limit || stopping)
				break;
		}
		last_completion_time = elapsed(start_time) / 1000;
		loga.logaLine(Loga.LOGA_ALWAYS,
				"Extra wait to see if any duplicates arrive");
		sleep(10); /* check if any duplicate messages arrive */
		loga.logaLine(Loga.LOGA_ALWAYS, arrivedCount
				+ " messages arrived out of " + expectedCount
				+ " expected, in " + elapsed(start_time) / 1000 + " seconds");
		return success(expectedCount);
	}

	int success(int count) {
		int rc = 1;

		if (errors > 0) {
			loga.logaLine(Loga.LOGA_ALWAYS,
					"Workload test failed because the callback had errors");
			rc = 0;
		}
		if (arrivedCount != count) {
			int qos = new Integer(options.get("qos"));
			if (qos == 2 || (qos == 1 && arrivedCount < count)) {
				loga.logaLine(Loga.LOGA_ALWAYS,
						"Workload test failed because the wrong number of messages"
								+ " was received: " + arrivedCount
								+ " whereas " + count + " were expected");
				rc = 0;
			}
		}
		if (rc == 1)
			control.send("verdict: pass");
		else
			control.send("verdict: fail");
		return rc;
	}

}
