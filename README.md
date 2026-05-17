# Streaming 02 Kafka

## Project Overview

This project demonstrates the foundations of streaming analytics using Apache Kafka, Python, producers,
consumers, and Kafka topics.

The main goal of this project is to show how data can move through a real-time streaming workflow.
In this module, the producer reads online sales records from a CSV file and publishes each record as
a Kafka message. The consumer subscribes to the Kafka topic, receives the messages, processes them,
and writes the results to an output CSV file.

This project builds on the first streaming foundations project by moving from a local simulated topic
file to an actual Kafka topic.

## Custom Project Files

For my custom project, I created and worked with the following files:

```text
src/streaming/kafka_admin_femi.py
src/streaming/kafka_producer_femi.py
src/streaming/kafka_consumer_femi.py


Dataset

The dataset used in this project is:

data/sales.csv

This dataset contains simulated online sales transaction records. Each row represents one customer order.

The dataset includes fields such as:

order_id
datetime
region_id
currency_code
product_id
unit_price
quantity
is_online
customer_id
is_new_customer
device_type
payment_method
referral_source
discount_code
customer_note

I used the original sales dataset. I did not modify the source CSV file itself. Instead, I modified the Kafka consumer so that it adds new analysis fields after messages are consumed.

Kafka Topic

The Kafka topic used for my custom project is:

streaming-02-kafka-femi

The Kafka admin file is used to verify that Kafka is reachable and that the topic exists before running the producer and consumer.

The admin file can be run with:

uv run python -m streaming.kafka_admin_femi

This step is important because the producer and consumer need the Kafka topic to exist before messages can be sent and read successfully.

Kafka Producer

The custom Kafka producer file is:

src/streaming/kafka_producer_femi.py

The producer reads records from:

data/sales.csv

Then it sends each sales record as a message to the Kafka topic:

streaming-02-kafka-femi

The producer sends one message at a time, with a short delay between messages. This simulates streaming data because the records move through the system gradually instead of being processed all at once.

The Kafka message key used by the producer is:

region_id

Examples of message keys include:

US-TX
CA-QC
US-CA
US-MO
CA-ON

Using region_id as the message key is helpful because it connects each sales message to a geographic sales region.

The producer can be run with:

uv run python -m streaming.kafka_producer_femi
Kafka Consumer

The custom Kafka consumer file is:

src/streaming/kafka_consumer_femi.py

The consumer subscribes to the Kafka topic and receives the sales messages produced by the producer.

The consumer receives the original sales record fields plus Kafka metadata fields such as:

_kafka_key
_kafka_partition
_kafka_offset

These Kafka metadata fields help show how Kafka handled each message. The _kafka_key shows the message key, the _kafka_partition shows which partition stored the message, and the _kafka_offset shows the message position in the topic.

The consumer writes processed records to:

data/output/consumed_sales_femi.csv

The consumer can be run with:

uv run python -m streaming.kafka_consumer_femi
Technical Modification

For my technical modification, I updated the Kafka consumer so it does more than simply consume and save raw messages.

I modified the consumer to calculate a new field:

sale_total

The sale_total field is calculated using:

quantity * unit_price

I also added another field:

order_priority

The order_priority field classifies each order as either:

high_value
standard

Orders with a sale_total greater than or equal to 100 are classified as:

high_value

Orders below 100 are classified as:

standard

This modification turns the consumer into a basic real-time analytics processor. Instead of only receiving messages, it adds business meaning to each sales transaction.

Apply the Skills to a New Problem

I applied the Kafka streaming workflow to a business problem: identifying high-value online sales orders in real time.

In a real business setting, a company may want to know when larger purchases happen instead of waiting for an end-of-day report. My modified consumer helps solve this problem by processing each sales message as it arrives and flagging higher-value orders immediately.

This type of streaming analytics could support:

real-time sales monitoring
customer behavior analysis
regional sales tracking
product demand analysis
high-value order detection
business reporting
How to Run the Project

Before running the Python files, make sure Kafka is running in WSL.

In the WSL Kafka terminal, start Kafka:

cd ~/kafka
bin/kafka-server-start.sh config/server.properties

Leave that terminal open while running the project.

Then run the project files in this order from the root project folder:

1. Run the Kafka admin file
uv run python -m streaming.kafka_admin_femi

This verifies the Kafka connection and ensures the topic exists.

2. Run the Kafka producer
uv run python -m streaming.kafka_producer_femi

This sends sales messages to the Kafka topic.

3. Run the Kafka consumer
uv run python -m streaming.kafka_consumer_femi

This consumes the messages, processes them, and writes the results to a CSV file.

Expected Output

The expected consumer output file is:

data/output/consumed_sales_femi.csv

This file should include the original sales fields, Kafka metadata fields, and the custom analysis fields:

sale_total
order_priority
Results

When I ran the admin file, Kafka was reachable and the topic was verified or created.

When I ran the producer, it read records from the sales dataset and sent six sales messages to the Kafka topic. Each message represented one online sales transaction.

When I ran the consumer, it consumed the Kafka messages, processed each record, calculated the sale total, classified the order priority, and saved the results to consumed_sales_femi.csv.

This confirmed that the Kafka workflow worked successfully from producer to topic to consumer.

Interpretation

This project helped me understand how Kafka supports streaming analytics. The producer and consumer do not directly call each other. Instead, the producer publishes messages to a Kafka topic, and the consumer subscribes to that topic to read and process the messages.

This showed me how Kafka helps decouple systems. Each part of the pipeline has its own role:

Admin: manages and verifies the topic
Producer: sends messages to Kafka
Topic: stores and organizes messages
Consumer: reads and processes messages

The biggest change from the original example was that my consumer added business-focused analysis fields. Instead of only saving raw messages, it calculated sale_total and classified orders as high_value or standard.

Business Intelligence Gained

The consumed messages can provide useful business insights, such as:

which products are being purchased
which regions are producing sales
which orders are high-value
which referral sources are bringing customers in
which device types customers are using
how much revenue each order represents

This kind of streaming workflow could help a business monitor activity as it happens and make faster decisions.

Challenges

One challenge I encountered was making sure Kafka was running before starting the producer and consumer. I also had to make sure the .env file was created correctly from .env.example.

Another challenge was making sure the correct Kafka topic was used. At first, the project used the topic from .env.example, but I needed to create a real .env file and set:

KAFKA_TOPIC=streaming-02-kafka-femi

I addressed these challenges by reading the terminal logs carefully, running the Kafka admin file first, and confirming that the topic existed before running the producer and consumer.

Suggestions for Others

My advice to others is to run the Kafka admin file first before running the producer and consumer. This helps confirm that Kafka is reachable and that the topic exists.

Also, make sure to create a .env file from .env.example. The .env.example file is only a template, but the Python files need the actual .env file to load the correct project settings.

A good run order is:

uv run python -m streaming.kafka_admin_femi
uv run python -m streaming.kafka_producer_femi
uv run python -m streaming.kafka_consumer_femi

Reading the logs carefully is also very helpful because the logs show whether Kafka is reachable, which topic is being used, how many messages were sent, and whether the consumer processed the messages successfully.





[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: send and receive Kafka messages.

Streaming analytics requires working with data in motion
and distributed, scalable systems.
This course builds capabilities through working projects.
In the age of generative AI, durable skills are grounded in real work:
setting up a professional environment,
reading and running code,
understanding the logic,
and pushing work to a shared repository.
Each project follows the structure of professional Python projects.
We learn by doing.

## This Project

This project introduces Kafka producers and consumers.

The project uses Kafka to move sales messages from a producer to a consumer.
The producer sends messages to a Kafka topic.
The consumer reads those messages one at a time and writes consumed records to CSV.

This module focuses on the basic producer-topic-consumer pattern.

The goal is to see messages move through Kafka before
introducing validation, analytics, visualization,
and storage/persistence.

## Working Files

You'll work with just these areas:

- **data/** - input data and generated output files
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - update authorship & links
- **zensical.toml** - update authorship & links

## Instructions

Follow the
[step-by-step workflow guide](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to complete:

1. Phase 1. **Start & Run**
2. Phase 2. **Change Authorship**
3. Phase 3. **Read & Understand**
4. Phase 4. **Modify**
5. Phase 5. **Apply**

## Challenges

Challenges are expected.
Sometimes instructions may not quite match your operating system.
When issues occur, share screenshots, error messages, and details about what you tried.
Working through issues is part of implementing professional projects.

## Success

After completing Phase 1. **Start & Run**, you'll have your own GitHub project
running with Kafka.

Use four named terminals:

1. **kafka** - keep the Kafka message broker running
2. **topics** - create, list, or reset Kafka topics
3. **producer** - run the project and producer
4. **consumer** - run the consumer

After the producer and consumer run successfully, you should see:

```shell
========================
Consumer executed successfully!
========================
```

A new file `project.log` will appear in the root project folder
and processed data will appear in data/output/.

## Command Reference

The commands below are used in the workflow guide above.
They are provided here for convenience.

**Important:** the first few times you run a project,
follow the guide with the **complete instructions**.

<details>
<summary>Show command reference</summary>

### In a machine terminal (open in your `Repos` folder)

After you get a copy of this repo in your own GitHub account,
open a machine terminal in your `Repos` folder:

```bash
# Replace username with YOUR GitHub username.
git clone https://github.com/Airfirm/streaming-02-kafka

cd streaming-02-kafka
code .
```

### In VS Code Terminal 1: Start Kafka (kafka)

For full instructions see
[**start kafka**](https://denisecase.github.io/pro-analytics-02/kafka/start-kafka/).

If any command fails,
repeat the steps at
[**install kafka**](https://denisecase.github.io/pro-analytics-02/kafka/install-kafka/)
until starting up is reliable.

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"

"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

### In VS Code terminal 2: Create Topic (topics)

For full instructions see
[**create topic**](https://denisecase.github.io/pro-analytics-02/kafka/create-topic/).

The topic name must match the name defined in your
`.env` file (copy `.env.example` to `.env`).

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-02-kafka-case
```

### In VS Code Terminal 3: Run Project and Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
# reset uv cache only if/when you start getting strange dependency errors
# uv cache clean

uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

uvx pre-commit install

git add -A
uvx pre-commit run --all-files
# repeat if changes were made
git add -A
uvx pre-commit run --all-files

# run the producer
clear
uv run python -m streaming.kafka_producer_femi

# do chores
uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build

# save progress
git add -A
git commit -m "update"
git push -u origin main
```

### In VS Code Terminal 4: Run Consumer (consumer)

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.
Clear the terminal, then start the consumer.

```shell
clear
uv run python -m streaming.kafka_consumer_femi
```

To start fresh, see
[manage topics](https://denisecase.github.io/pro-analytics-02/kafka/manage-topics/)
to delete the topic and recreate it.

</details>

## Notes

- Use the **UP ARROW** and **DOWN ARROW** in the terminal to scroll through past commands.
- Use `CTRL+f` to find (and replace) text within a file.
- You do not need to add to or modify `tests/`. They are provided for example only.
- Many files are silent helpers. Explore as you like, but nothing is required.
- You do NOT not to understand everything; understanding builds naturally over time.

## Troubleshooting >>> or

If you see something like this in your terminal: `>>>` or `...`
You accidentally started Python interactive mode.
It happens.
Press `Ctrl+c` (both keys together) or `Ctrl+Z` then `Enter` on Windows.

## Producer Example Output

Note: Kafka uses a lower-level client library called `rdkafka`.
that may send `FAIL` messages while trying connection paths.
It often figures it out and continues.

```text
| P02 | ========================
| P02 | START producer main()
| P02 | ========================
| P02 | ROOT_DIR = .
| P02 | DATA_DIR = data
| P02 | SALES_CSV = data\sales.csv
| P02 | ========================
| P02 | SECTION A. Acquire
| P02 | ========================
| P02 | Loading settings from .env...
| P02 | KAFKA_BOOTSTRAP_SERVERS           = localhost:9092
| P02 | KAFKA_TOPIC                       = streaming-02-kafka-case
| P02 | PRODUCER_MESSAGE_COUNT            = 3
| P02 | PRODUCER_MESSAGE_INTERVAL_SECONDS = 2.0
| P02 | KAFKA_CLEAR_TOPIC_ON_START        = True
| P02 | Verifying Kafka connection...
| P02 | Kafka port is reachable.
%3|1778412238.257|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2041ms in state CONNECT)
%3|1778412240.313|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2021ms in state CONNECT, 1 identical error(s) suppressed)
| P02 | ========================
| P02 | SECTION P. Produce Messages
| P02 | ========================
| P02 | Sending messages...
| P02 | Sending up to 3 message(s) to topic 'streaming-02-kafka-case'.
| P02 | Watch each sale arrive. Press CTRL+C to stop early.

| P02 | {
  order_id: e7324981-a9f0-419f-b708-d0a333451fff
  datetime: 2026-05-04T08:11:00Z
  region_id: US-TX
  currency_code: USD
  product_id: PY-STREAM-005
  unit_price: 59.99
  quantity: 3
  is_online: true
  customer_id: CUST-4150
  is_new_customer: false
  device_type: tablet
  payment_method: paypal
  referral_source: paid_search
  discount_code:
  customer_note: Gift for my team
}
| P02 |   Sending message with key=US-TX
| P02 |   MESSAGE SENT  sent=1
| P02 | {
  order_id: d61943e0-f543-4b5f-9c9a-18605ea4cfe5
  datetime: 2026-05-04T08:23:00Z
  region_id: US-TX
  currency_code: USD
  product_id: PY-DATA-002
  unit_price: 49.99
  quantity: 1
  is_online: true
  customer_id: CUST-1106
  is_new_customer: false
  device_type: mobile
  payment_method: paypal
  referral_source: paid_search
  discount_code:
  customer_note: Gift for my team
}
| P02 |   Sending message with key=US-TX
| P02 |   MESSAGE SENT  sent=2
| P02 | {
  order_id: 14da1915-8e74-47be-9e10-f7275d31af46
  datetime: 2026-05-04T08:28:00Z
  region_id: CA-QC
  currency_code: CAD
  product_id: PY-NLP-006
  unit_price: 54.99
  quantity: 1
  is_online: true
  customer_id: CUST-2133
  is_new_customer: false
  device_type: desktop
  payment_method: paypal
  referral_source: organic
  discount_code:
  customer_note: Learning at my own pace
}
| P02 |   Sending message with key=CA-QC
| P02 |   MESSAGE SENT  sent=3
| P02 | ========================
| P02 | SECTION E. Exit
| P02 | ========================
| P02 | Summary:
| P02 | Sent 3 message(s) from topic 'streaming-02-kafka-case'.
| P02 | ========================
| P02 | Producer executed successfully!
| P02 | ========================
```

## Consumer Example Output

Note: Kafka uses a lower-level client library called `rdkafka`.
that may send `FAIL` messages while trying connection paths.
It often figures it out and continues.

```text
| C02 | ========================
| C02 | START consumer main()
| C02 | ========================
| C02 | ROOT_DIR = .
| C02 | DATA_DIR = data
| C02 | OUTPUT_CSV = data\output\consumed_sales.csv
| C02 | ========================
| C02 | SECTION A. Acquire
| C02 | ========================
| C02 | Loading settings from .env...
| C02 | KAFKA_BOOTSTRAP_SERVERS  = localhost:9092
| C02 | KAFKA_TOPIC              = streaming-02-kafka-case
| C02 | KAFKA_GROUP_ID           = streaming-consumer-group-A
| C02 | CONSUMER_TIMEOUT_SECONDS = 10.0
| C02 | CONSUMER_MAX_MESSAGES    = 1000
| C02 | Verifying Kafka connection...
| C02 | Kafka port is reachable.
| C02 | Verifying Kafka topic...
%3|1778412256.877|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2027ms in state CONNECT)
%3|1778412258.963|FAIL|rdkafka#producer-1| [thrd:localhost:9092/bootstrap]: localhost:9092/bootstrap: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2038ms in state CONNECT, 1 identical error(s) suppressed)
%3|1778412261.050|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2026ms in state CONNECT)
%3|1778412263.132|FAIL|rdkafka#producer-1| [thrd:localhost:9092/1]: localhost:9092/1: Connect to ipv4#127.0.0.1:9092 failed: Unknown error (after 2025ms in state CONNECT, 1 identical error(s) suppressed)
| C02 | Topic 'streaming-02-kafka-case' exists.
| C02 | Found 3 message(s) available.
| C02 | Creating Kafka consumer...
| C02 | Subscribed to topic: 'streaming-02-kafka-case' (reading from beginning)
| C02 | ========================
| C02 | SECTION C. Consume and Process Messages
| C02 | ========================
| C02 | Initializing output...
| C02 | Output CSV cleared: consumed_sales.csv
| C02 | Consuming messages...
| C02 | Waiting for up to 1000 message(s).
| C02 | Press CTRL+C to stop early.

| C02 | {'currency_code': 'USD', 'customer_id': 'CUST-4150', 'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:11:00Z', 'device_type': 'tablet', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': 'e7324981-a9f0-419f-b708-d0a333451fff', 'payment_method': 'paypal', 'product_id': 'PY-STREAM-005', 'quantity': '3', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '59.99', '_kafka_key': 'US-TX', '_kafka_partition': 0, '_kafka_offset': 0}
| C02 | Processing raw message.
| C02 | MESSAGE CONSUMED
| C02 | consumed=1
| C02 | {'currency_code': 'USD', 'customer_id': 'CUST-1106', 'customer_note': 'Gift for my team', 'datetime': '2026-05-04T08:23:00Z', 'device_type': 'mobile', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': 'd61943e0-f543-4b5f-9c9a-18605ea4cfe5', 'payment_method': 'paypal', 'product_id': 'PY-DATA-002', 'quantity': '1', 'referral_source': 'paid_search', 'region_id': 'US-TX', 'unit_price': '49.99', '_kafka_key': 'US-TX', '_kafka_partition': 0, '_kafka_offset': 1}
| C02 | Processing raw message.
| C02 | MESSAGE CONSUMED
| C02 | consumed=2
| C02 | {'currency_code': 'CAD', 'customer_id': 'CUST-2133', 'customer_note': 'Learning at my own pace', 'datetime': '2026-05-04T08:28:00Z', 'device_type': 'desktop', 'discount_code': '', 'is_new_customer': 'false', 'is_online': 'true', 'order_id': '14da1915-8e74-47be-9e10-f7275d31af46', 'payment_method': 'paypal', 'product_id': 'PY-NLP-006', 'quantity': '1', 'referral_source': 'organic', 'region_id': 'CA-QC', 'unit_price': '54.99', '_kafka_key': 'CA-QC', '_kafka_partition': 0, '_kafka_offset': 2}
| C02 | Processing raw message.
| C02 | MESSAGE CONSUMED
| C02 | consumed=3
| C02 | No message received within 10.0s timeout.
| C02 | Producer finished or paused. Stopping consumer.
| C02 | Kafka consumer closed.
| C02 | ========================
| C02 | SECTION E. Exit
| C02 | ========================
| C02 | Summary:
| C02 | Consumed 3 message(s) from topic 'streaming-02-kafka-case'.
| C02 | OUTPUT_CSV = data\output\consumed_sales.csv
| C02 | ========================
| C02 | Consumer executed successfully!
| C02 | ========================
```
