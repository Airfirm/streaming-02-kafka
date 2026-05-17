# Streaming Data

## Custom Project

### Dataset

For this custom Kafka project, I used the original sales dataset file:

`data/sales.csv`

This dataset contains simulated online sales transaction records.
Each record represents one customer order from an e-commerce style sales system.
The records include information about the customer, product, order, device type,
payment method, and referral source.
They also include region, quantity, and unit price.

The fields included in each record are:

- `order_id`
- `datetime`
- `region_id`
- `currency_code`
- `product_id`
- `unit_price`
- `quantity`
- `is_online`
- `customer_id`
- `is_new_customer`
- `device_type`
- `payment_method`
- `referral_source`
- `discount_code`
- `customer_note`

I used the original sales dataset, but I modified the Kafka consumer processing so the
consumed output adds new analyst-friendly fields. The dataset itself still represents
online product sales activity.

### Kafka Messages

My Kafka producer reads `data/sales.csv` and sends each record to a Kafka topic.

The Kafka topic I used for my custom project is:

`streaming-02-kafka-femi`

Each message represents one sales transaction. The producer sends the full sales record,
including customer, order, product, region, payment, and pricing information.

The message key I used is the `region_id` field. For example, some message keys include:

- `US-TX`
- `CA-QC`
- `US-CA`
- `US-MO`
- `CA-ON`

Using `region_id` as the Kafka message key is useful because it connects each message
to a geographic sales region.
In a real business setting, this could help organize or analyze sales activity by location.

I did not change the fields sent by the producer.
The producer sends the original sales fields from the CSV file.
The additional analysis fields are added later by the consumer.

### Consumer Processing

My Kafka consumer receives sales messages from the Kafka topic and processes each message one at a time.

The consumer receives the original sales transaction fields from Kafka,
along with Kafka metadata fields such as:

- `_kafka_key`
- `_kafka_partition`
- `_kafka_offset`

These Kafka metadata fields help show how Kafka handled each message.
The `_kafka_key` shows the message key.
The `_kafka_partition` shows which partition stored the message.
The `_kafka_offset` shows the message position in the topic.

My consumer is designed to consume up to the maximum number of messages listed in the `.env` settings.
For this run, the producer sent 6 messages, so the consumer should consume 6 sales records.

The consumer logs each message as it is received and processed. It also writes the consumed and processed
records to this CSV file:

`data/output/consumed_sales_femi.csv`

    For my technical modification, I updated the consumer so it processes selected
    fields from each sales message. Specifically, it uses:

`sale_total`

This field is calculated by multiplying `quantity` by `unit_price`.

The consumer also adds a new field called:

`order_priority`

Orders with a `sale_total` greater than or equal to 100 are labeled:

`high_value`

Orders below 100 are labeled:

`standard`

This makes the consumed output more useful for analysis because it does not only store raw messages.
It adds business meaning to each transaction.

### Experiments

For my Phase 4 technical change, I customized the Kafka project files and settings.
I created custom versions of the Kafka files using my name:

- `kafka_admin_femi.py`
- `kafka_producer_femi.py`
- `kafka_consumer_femi.py`

I also worked with the Kafka topic settings so my project could use a custom topic name:

`streaming-02-kafka-femi`

The admin file is important because it verifies the Kafka connection.
It also makes sure the topic exists before the producer and consumer are run.

For my Phase 5 application, I applied the streaming workflow to a new business problem:
identifying high-value online sales orders in real time.

I modified the Kafka consumer so that each consumed message is processed to calculate `sale_total`.
It also assigns an `order_priority`.
This turns the consumer from a simple message reader into a basic real-time analytics processor.

This application could help a business monitor important sales activity as it
happens. Instead of waiting for an end-of-day batch report, the business could
identify larger purchases immediately.

### Results

When I ran the Kafka admin file, it verified that Kafka was reachable and confirmed that the topic existed.
It created the topic if needed.

When I ran the Kafka producer, it read the records from `data/sales.csv`
and sent 6 sales messages to the Kafka topic.
Each message was sent one at a time with a short delay between messages.
This simulated streaming data.

When I ran the Kafka consumer, it read the messages from the Kafka topic and processed each record.
The consumer wrote the final processed results to:

`data/output/consumed_sales_femi.csv`

The consumed output included the original sales fields, Kafka metadata fields,
and my custom analysis fields:

- `sale_total`
- `order_priority`

This showed that the streaming workflow was working from start to finish:

1. The admin file verified or created the topic.
2. The producer sent sales messages to Kafka.
3. The consumer read the messages from Kafka.
4. The consumer processed the messages.
5. The processed records were saved to a CSV file.

### Interpretation

This Kafka streaming workflow showed me how producers, topics, and consumers work together
in a real-time data pipeline.

The original example focused mainly on sending and receiving Kafka messages.
My custom version added a business-focused processing step in the consumer.

Instead of only saving the raw consumed message, my consumer calculated the total sale amount and
classified each order as either `high_value` or `standard`.

Watching messages move through Kafka helped me understand that the producer and consumer are separate
parts of the system.

The producer does not directly call the consumer.
Instead, the producer sends messages to a Kafka topic, and the consumer subscribes to that topic to
read the messages. This shows how Kafka helps decouple systems.

This stream could tell a business which products are being purchased, which regions are active,
what devices customers are using, which referral sources are bringing customers in, and which
orders are high-value. For example, a business could monitor whether customers are coming from
paid search or organic traffic, which regions are generating sales, and which products are being
purchased most often.

The business intelligence gained from the consumed messages includes:

- tracking online sales activity in real time
- identifying high-value orders
- analyzing sales by region
- comparing referral sources such as paid search and organic traffic
- observing customer device types such as mobile, tablet, and desktop
- understanding product demand from the product IDs
- supporting faster decision-making based on live sales activity

Overall, this project helped me understand how Kafka can support streaming analytics.
It also showed me how an analyst can add value by processing messages as they arrive and turning raw
streaming data into useful business insights.

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy from examples and make them yours
- **Glossary** - project terms and concepts
- **API** - autogenerated look at the code interface
