package no.hvl.dat110.iotsystem;

import no.hvl.dat110.client.Client;
import no.hvl.dat110.common.TODO;
import no.hvl.dat110.messages.PublishMsg;

public class TemperatureDevice {

	private static final int COUNT = 10;

	public static void main(String[] args) {

		// simulated / virtual temperature sensor
		TemperatureSensor sn = new TemperatureSensor();

		// TODO - start

		Client client = new Client("temperaturedevice", Common.BROKERHOST, Common.BROKERPORT);
		client.connect();
		
		client.createTopic(Common.TEMPTOPIC);
		
		client.subscribe(Common.TEMPTOPIC);
		
		for (int i = 0; i < COUNT; i++) {
			int temp = sn.read();
			client.publish(Common.TEMPTOPIC, Integer.toString(temp));
			System.out.println("\nTemperature: " + temp);
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		client.unsubscribe(Common.TEMPTOPIC);
		
		client.disconnect();

		// TODO - end

		System.out.println("Temperature device stopping ... ");

		throw new UnsupportedOperationException(TODO.method());

	}
}
