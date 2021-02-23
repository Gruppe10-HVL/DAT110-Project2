package no.hvl.dat110.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messagetransport.Connection;

public class Storage {

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions;
	protected ConcurrentHashMap<String, ClientSession> clients;
	// data structure for managing currently connected clients
	// maps from user to corresponding client session object

	// Buffer implementation for discunnected users
	protected ConcurrentHashMap<String, Boolean> connected;
	protected ConcurrentHashMap<String, ArrayList<Message>> messageBuffers;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {

		return subscriptions.keySet();

	}

	// get the session object for a given user
	// session object can be used to send a message to the user

	public ClientSession getSession(String user) {

		ClientSession session = clients.get(user);

		return session;
	}

	public Set<String> getSubscribers(String topic) {

		return (subscriptions.get(topic));

	}

	public void addClientSession(String user, Connection connection) {

		// TODO: add corresponding client session to the storage

		clients.put(user, new ClientSession(user, connection));

	}

	public void removeClientSession(String user) {

		clients.remove(user);

	}

	/**
	 * 
	 * @param user
	 */
	public void disconnectUser(String user) {
		connected.put(user, false);
		clients.get(user).disconnect();
	}

	/**
	 * 
	 * @param user
	 * @param connection
	 */
	public void connectUser(String user, Connection connection) {
		connected.put(user, true);
		clients.put(user, new ClientSession(user, connection));
	}

	/**
	 * 
	 * @param user
	 */
	public boolean isConnected(String user) {
		return connected.get(user);
	}

	/**
	 * 
	 * @param user
	 * @param msg
	 */
	public void addMessageToBuffer(String user, Message msg) {
		messageBuffers.get(user).add(msg);
	}

	/**
	 * 
	 * @param user
	 * @return
	 */
	public ArrayList<Message> getMessageBuffer(String user) {
		return messageBuffers.get(user);
	}

	public void emptyMessageBuffer(String user) {
		messageBuffers.get(user).clear();
	}

	public void createTopic(String topic) {

		// TODO: create topic in the storage
		Set<String> set = new HashSet<>();
		subscriptions.put(topic, set);
	}

	public void deleteTopic(String topic) {

		// TODO: delete topic from the storage

		subscriptions.remove(topic);
	}

	public void addSubscriber(String user, String topic) {

		// TODO: add the user as subscriber to the topic

		Set<String> set = getSubscribers(topic);
		set.add(user);
		subscriptions.put(topic, set);
	}

	public void removeSubscriber(String user, String topic) {

		// TODO: remove the user as subscriber to the topic

		Set<String> set = getSubscribers(topic);
		set.remove(user);
		subscriptions.put(topic, set);

	}
}
