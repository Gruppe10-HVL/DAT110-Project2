package no.hvl.dat110.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import no.hvl.dat110.messages.Message;
import no.hvl.dat110.messages.PublishMsg;
import no.hvl.dat110.messagetransport.Connection;

public class Storage {

	// data structure for managing subscriptions
	// maps from a topic to set of subscribed users
	protected ConcurrentHashMap<String, Set<String>> subscriptions;
	protected ConcurrentHashMap<String, ClientSession> clients;
	// data structure for managing currently connected clients
	// maps from user to corresponding client session object

	// Buffer implementation for discunnected users
	protected ConcurrentHashMap<String, Queue<Message>> bufferedMessages;

	public Storage() {
		subscriptions = new ConcurrentHashMap<String, Set<String>>();
		clients = new ConcurrentHashMap<String, ClientSession>();
		bufferedMessages = new ConcurrentHashMap<String, Queue<Message>>();
	}

	public Collection<ClientSession> getSessions() {
		return clients.values();
	}

	public Set<String> getTopics() {

		return subscriptions.keySet();

	}

	/**
	 * 
	 * @param user
	 * @return
	 */
	public Queue<Message> getBufferedMessages(String user) {
		return bufferedMessages.get(user);
	}

	/**
	 * 
	 * @param user
	 */
	public void newMessageQueue(String user) {
		bufferedMessages.put(user, new LinkedList<Message>());
	}

	/**
	 * 
	 * @param msg
	 */
	public void addMessageToQueue (PublishMsg msg) {
		Set<String> users = subscriptions.get(msg.getTopic());
		if (users != null) {
			users.stream().forEach(user -> {
				if (bufferedMessages.containsKey(user)) {
					Queue<Message> messages = bufferedMessages.get(user);
					messages.add(msg);
					bufferedMessages.put(user, messages);
				}
			});
		}
	}

	/**
	 * 
	 */
	public void clearMessageQueue(String user) {
		bufferedMessages.remove(user);
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
