package no.hvl.dat110.broker;

import java.util.Collection;
import java.util.Queue;
import java.util.Set;

import no.hvl.dat110.common.Logger;
import no.hvl.dat110.common.Stopable;
import no.hvl.dat110.messages.*;
import no.hvl.dat110.messagetransport.Connection;

public class Dispatcher extends Stopable {

	private Storage storage;

	public Dispatcher(Storage storage) {
		super("Dispatcher");
		this.storage = storage;

	}

	@Override
	public void doProcess() {

		Collection<ClientSession> clients = storage.getSessions();

		Logger.lg(".");
		for (ClientSession client : clients) {

			Message msg = null;

			if (client.hasData()) {
				msg = client.receive();
			}

			// a message was received
			if (msg != null) {
				dispatch(client, msg);
			}
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void dispatch(ClientSession client, Message msg) {

		MessageType type = msg.getType();

		// invoke the appropriate handler method
		switch (type) {

		case DISCONNECT:
			onDisconnect((DisconnectMsg) msg);
			break;

		case CREATETOPIC:
			onCreateTopic((CreateTopicMsg) msg);
			break;

		case DELETETOPIC:
			onDeleteTopic((DeleteTopicMsg) msg);
			break;

		case SUBSCRIBE:
			onSubscribe((SubscribeMsg) msg);
			break;

		case UNSUBSCRIBE:
			onUnsubscribe((UnsubscribeMsg) msg);
			break;

		case PUBLISH:
			onPublish((PublishMsg) msg);
			break;

		default:
			Logger.log("broker dispatch - unhandled message type");
			break;

		}
	}

	// called from Broker after having established the underlying connection
	public void onConnect(ConnectMsg msg, Connection connection) {
		String user = msg.getUser();

		Logger.log("onConnect:" + msg.toString());

		storage.addClientSession(user, connection);

		Queue<Message> messages = storage.getBufferedMessages(user);
		
		if (messages != null) {
			messages.stream().forEach(message -> storage.getSession(user).send(message));
		}

		storage.clearMessageQueue(user);
	}

	// called by dispatch upon receiving a disconnect message
	public void onDisconnect(DisconnectMsg msg) {
		String user = msg.getUser();

		Logger.log("onDisconnect:" + msg.toString());

		storage.newMessageQueue(user);
		
		storage.removeClientSession(user);
	}

	public void onCreateTopic(CreateTopicMsg msg) {
		Logger.log("onCreateTopic:" + msg.toString());

		try {
			storage.createTopic(msg.getTopic());			
		} catch (NullPointerException e) {
			e.printStackTrace();
			Logger.log("Error");
		}

	}

	public void onDeleteTopic(DeleteTopicMsg msg) {
		Logger.log("onDeleteTopic:" + msg.toString());

		try {
			storage.deleteTopic(msg.getTopic());			
		} catch (NullPointerException e) {
			e.printStackTrace();
			Logger.log("Error");
		}

	}

	public void onSubscribe(SubscribeMsg msg) {
		Logger.log("onSubscribe:" + msg.toString());

		try {
			storage.addSubscriber(msg.getUser(), msg.getTopic());
		} catch (NullPointerException e) {
			e.printStackTrace();
			Logger.log("Error");
		}

	}

	public void onUnsubscribe(UnsubscribeMsg msg) {
		Logger.log("onUnsubscribe:" + msg.toString());
		
		try {
			storage.removeSubscriber(msg.getUser(), msg.getTopic());
		} catch (NullPointerException e) {
			e.printStackTrace();
			Logger.log("Error");
		}

	}

	public void onPublish(PublishMsg msg) {
		Logger.log("onPublish:" + msg.toString());

		Set<String> subscribers = storage.getSubscribers(msg.getTopic());
		
		if (subscribers != null) {
			subscribers.stream().forEach(user -> {
				if (storage.getSession(user) != null) {
					storage.getSession(user).send(msg);
				}
			});
		}
	}
}
