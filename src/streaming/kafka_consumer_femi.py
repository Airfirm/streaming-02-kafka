"""src/streaming/kafka_consumer_femi.py.

Kafka consumer: full pipeline example.

Reads sales messages from a Kafka topic.

Start with main() at the bottom.
Work up to see how it all fits together.

Technical Modification:
This custom version applies streaming analytics to a new business problem:
identifying high-value online sales orders in real time.

The consumer now:
- Consumes sales messages from a Kafka topic.
- Calculates sale_total using quantity * unit_price.
- Adds order_priority:
    - high_value if sale_total is greater than or equal to 100
    - standard if sale_total is less than 100
- Writes processed records to consumed_sales_femi.csv.

Author: Oluwafemi Salawu
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_femi
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("C02", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# Business rule for high-value orders
HIGH_VALUE_THRESHOLD: Final[float] = 100.00

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

# Custom output file for Femi's processed Kafka consumer results
OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_sales_femi.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C02")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    LOG.info(f"HIGH_VALUE_THRESHOLD     = {HIGH_VALUE_THRESHOLD}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")

    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the Kafka admin file first to create the topic.")
        LOG.error("Then run the producer before running the consumer.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")

    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )

    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output directory, CSV file, and running stats.

    Returns:
        A RunningStats instance.
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    return RunningStats()


def calculate_sale_total(row: dict[str, Any]) -> float:
    """Calculate the total sale amount for one order.

    Arguments:
        row: A consumed Kafka sales message.

    Returns:
        The calculated sale total rounded to two decimals.
    """
    try:
        quantity = int(row.get("quantity", 0))
        unit_price = float(row.get("unit_price", 0.0))
        sale_total = quantity * unit_price
    except TypeError:
        LOG.warning("Could not calculate sale_total. Defaulting to 0.00.")
        sale_total = 0.0
    except ValueError:
        LOG.warning("Could not calculate sale_total. Defaulting to 0.00.")
        sale_total = 0.0

    return round(sale_total, 2)


def classify_order_priority(sale_total: float) -> str:
    """Classify an order as high_value or standard.

    Arguments:
        sale_total: The calculated total sale amount.

    Returns:
        The order priority classification.
    """
    if sale_total >= HIGH_VALUE_THRESHOLD:
        return "high_value"

    return "standard"


def process_message(row: dict[str, Any]) -> dict[str, Any]:
    """Process one consumed Kafka message by adding analyst-friendly fields.

    This modification applies streaming analytics to a new business problem:
    identifying high-value online sales orders in real time.

    Arguments:
        row: A raw consumed Kafka message row.

    Returns:
        The processed row with sale_total and order_priority added.
    """
    LOG.info("Processing raw message.")

    processed_row = dict(row)

    sale_total = calculate_sale_total(processed_row)
    order_priority = classify_order_priority(sale_total)

    processed_row["sale_total"] = sale_total
    processed_row["order_priority"] = order_priority

    if order_priority == "high_value":
        LOG.info(f"HIGH VALUE ORDER DETECTED: ${sale_total:.2f}")
    else:
        LOG.info(f"Standard order processed: ${sale_total:.2f}")

    return processed_row


def consume_messages(consumer: Any) -> int:
    """Consume raw messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.

    Returns:
        The number of consumed messages.
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0

    while consumed_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        processed = process_message(row)

        append_csv_row(
            path=OUTPUT_CSV,
            row=processed,
            fieldnames=list(processed.keys()),
        )

        consumed_count += 1
        LOG.info("MESSAGE CONSUMED")
        LOG.info(f"consumed={consumed_count}")

    return consumed_count


def save_artifacts(stats: RunningStats) -> None:
    """Save output artifacts.

    Arguments:
        stats: A RunningStats instance for future analytics extensions.
    """
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(consumed_count: int, settings: KafkaSettings) -> None:
    """Log final summary statistics.

    Arguments:
        consumed_count: The number of messages consumed.
        settings: The KafkaSettings object containing configuration details.
    """
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    stats = initialize_output()

    consumed_count = 0

    try:
        consumed_count = consume_messages(consumer)
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    save_artifacts(stats)
    log_summary(consumed_count, settings)


# === CONDITIONAL EXECUTION GUARD ===

# WHY: If running this file as a script, then call main().
# This is standard Python "boilerplate".

if __name__ == "__main__":
    main()
