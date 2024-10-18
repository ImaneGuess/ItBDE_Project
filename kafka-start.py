import subprocess
import time
import os
import multiprocessing

# Kafka and Zookeeper services
ZOOKEEPER_SERVICE = 'zookeeper.service'
KAFKA_SERVICE = 'kafka.service'
KAFKA_HOME = os.path.expanduser('/usr/local/kafka')

def start_service(service_name):
    """Start a systemctl service."""
    output = subprocess.run(["systemctl", "status", service_name], capture_output=True)
    status = output.stdout.decode().strip()
    if "inactive" in status:
        print(f"Starting {service_name}...")
        subprocess.run(["sudo", "systemctl", "start", service_name])
        time.sleep(5)
    else:
        print(f"{service_name} is already running.")

def open_new_tab(command, title=""):
    """Open a new tab in gnome-terminal and run the specified command."""
    subprocess.Popen(["gnome-terminal", "--tab", "--title", title, "--", "bash", "-c", command])


def start_kafka_server(server_id):
    """Start a Kafka broker in a new terminal."""
    print(f"Starting Kafka broker {server_id}...")
    server_file = f"{KAFKA_HOME}/config/server-{server_id}.properties"
    if not os.path.exists(server_file):
        print(f"Error: Kafka server configuration file '{server_file}' not found.")
        return
    subprocess.Popen(["gnome-terminal", "--tab", "--title", f'Kafka Broker {server_id}', "--", "bash", "-c", 
                      f"{KAFKA_HOME}/bin/kafka-server-start.sh {server_file}; exec bash"])
    time.sleep(5)

def list_topics():
    """List all available Kafka topics."""
    print("Listing all available topics...")
    result = subprocess.run([f"{KAFKA_HOME}/bin/kafka-topics.sh", 
                             "--list", 
                             "--bootstrap-server", "localhost:9092"], 
                            capture_output=True, text=True)

    if result.returncode == 0:
        topics = result.stdout.strip().split('\n')
        if topics:
            print("Available topics:")
            for topic in topics:
                print(f"- {topic}")
        else:
            print("No topics available.")
    else:
        print(f"Error listing topics: {result.stderr}")

def create_topic(topic_name, replication_factor=1):
    """Create a Kafka topic on the chosen broker and list all topics on active servers."""
    print(f"Creating topic '{topic_name}'...")

    # Ask user which broker to create the topic on
    broker_id = input("Enter the broker ID on which to create the topic (0, 1, or 2): ")
    if broker_id not in ['0', '1', '2']:
        print("Invalid broker ID. Please enter 0, 1, or 2.")
        return

    # Create the topic on the specified Kafka broker
    subprocess.run([f"{KAFKA_HOME}/bin/kafka-topics.sh",
                    "--create", 
                    "--bootstrap-server", f"localhost:{9092 + int(broker_id)}", 
                    "--replication-factor", str(replication_factor), 
                    "--topic", topic_name])
    
    # List topics on all active Kafka brokers
    print("\nListing topics on all active servers:")
    for broker_id in range(3):
        print(f"\nKafka Broker {broker_id} topics:")
        subprocess.run([f"{KAFKA_HOME}/bin/kafka-topics.sh", 
                        "--list", 
                        "--bootstrap-server", f"localhost:{9092 + broker_id}"])

def send_test_message(topic_name):
    """Send a test message to a topic."""
    print(f"Sending test message to topic '{topic_name}'...")
    subprocess.run([f"{KAFKA_HOME}/bin/kafka-console-producer.sh", 
                    "--broker-list", "localhost:9092", 
                    "--topic", topic_name])

def consume_test_message(topic_name):
    """Consume messages from a topic."""
    print(f"Consuming messages from topic '{topic_name}'...")
    subprocess.run([f"{KAFKA_HOME}/bin/kafka-console-consumer.sh", 
                    "--bootstrap-server", "localhost:9092", 
                    "--topic", topic_name, 
                    "--from-beginning"])
    
def run_producer_and_consumer(topic_name):
    """Run producer and consumer concurrently."""
    consumer_process = multiprocessing.Process(target=consume_test_message, args=(topic_name,))
    consumer_process.start()
    send_test_message(topic_name)
    consumer_process.join()

def main():
    # Step 1: Start Zookeeper and Kafka services
    start_service(ZOOKEEPER_SERVICE)
    start_service(KAFKA_SERVICE)

    # Step 2: Start Kafka brokers
    for broker_id in range(3):
        start_kafka_server(broker_id)

    # Step 3: User interaction for topic management
    while True:
        print("\nKafka Management Script")
        print("1. List Topics")
        print("2. Create a Topic")
        print("3. Produce and Consume Test Message")
        print("4. Exit")

        choice = input("Enter your choice: ")
        
        if choice == '1':
            list_topics()

        elif choice == '2':
            topic_name = input("Enter topic name: ")
            replication_factor = input("Enter replication factor (default=1): ") or 1
            create_topic(topic_name, int(replication_factor))

        elif choice == '3':
            topic_name = input("Enter the topic name to send and consume messages (for testing purposes): ")
            run_producer_and_consumer(topic_name)

        elif choice == '4':
            print("Exiting Kafka Management Script.")
            break

        else:
            print("Invalid choice. Please select again.")

if __name__ == "__main__":
    main()