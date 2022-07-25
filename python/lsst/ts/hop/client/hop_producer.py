# This file is part of ts_hop_client.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = [
    "HopProducer",
    "run_hop_producer",
]

import hop
import asyncio
import logging
import argparse

from lsst.ts import utils

from lsst.ts.salkafka.kafka_producer_factory import (
    KafkaConfiguration,
    KafkaProducerFactory,
)

from .make_avro_schema import (
    make_avro_schema_heartbeat,
    make_avro_schema_heartbeat_message,
)


class HopProducer:
    """Produce kafka messages from SCiMMA Hopskotch kafka topics.

    Parameters
    ----------
    kafka_factory : `KafkaProducerFactory`
        Information and clients for using Kafka.
    rubing_topic : `str`, optional
        (TODO)
    scimma_hostname : `str`, optional
        Hopskotch hostname to subscribe to. Production instance is
        kafka.scimma.org; development instance is dev.hop.scimma.org
    auth : `hop.Auth` or `False`, optional
        auth object to use to authenticate to a SCiMMA topic. If False, no
        authentication is attempted. Default is to use any existing credentials
        associated with the current hop-client installation.
    log : `logging.Logger` or `None`, optional
        Optional logging class to be used for logging operations. If
        `None`, creates a new logger.
    """

    def __init__(
        self,
        kafka_factory,
        rubin_topic="rubin.testing-schema",
        scimma_hostname="kafka.scimma.org",
        auth="current",
        log=None,
    ):

        self.kafka_factory = kafka_factory

        self.log = (
            logging.getLogger(type(self).__name__)
            if log is None
            else log.getChild(type(self).__name__)
        )

        self.rubin_topic = rubin_topic
        self.scimma_hostname = scimma_hostname
        if auth == "current":
            self.auth = hop.auth.select_matching_auth(
                hop.auth.load_auth(), scimma_hostname
            )
        self.avro_schema = make_avro_schema_heartbeat()

        self.kafka_producer = None
        self.kafka_factory.make_kafka_topics([self.avro_schema["name"]])

        self.start_task = asyncio.ensure_future(self.start())

        self.done_task = utils.make_done_future()

    def _read_topic(
        self,
        topic="sys.heartbeat",
        start_offset=hop.io.consumer.ConsumerStartPosition.LATEST,
    ):
        """Start streaming messages from a kafka topic.

        Parameters
        ----------
        topic : `str`
            kafka topic from which to stream. Defaults to Hopskotch system
            heartbeat.
        start_offset : `hop.io.consumer.ConsumerStartPosition`
            kafka topic offset from which to start the stream at (EARLIEST or
            LATEST). Defaults to the latest offest, so only new messages are
            streamed.
        """
        if self.done_task.done():
            raise RuntimeError("Execution not started or completed. Run start again.")

        stream = hop.Stream(auth=self.auth, start_at=start_offset)
        try:
            with stream.open(f"kafka://{self.scimma_hostname}/{topic}", "r") as s:
                for message in s:
                    avro_data = make_avro_schema_heartbeat_message(message=message)
                    self.log.debug(f"Sending message: {message!r}")
                    asyncio.run(
                        self.kafka_producer.send_and_wait(
                            self.avro_schema["name"], value=avro_data
                        )
                    )
        except Exception as e:
            self.log.exception("Error reading/sending scimma topic.")
            self.done_task.set_exception(e)
        else:
            self.log.info("Exiting read topic loop, setting done task.")
            self.done_task.set_result(True)

    def _message_printing(self, message):
        """Placeholder function for processing a message. Currently just print
        the message.

        Parameters
        ----------
        message : `hop.models.*`
            hop message model object received from a hop stream object.
        """
        print(message)

    def _message_relay(self, message, avro_schema):
        """Placeholder function for translating a heartbeat message into avro
        and relaying it to another topic.
        """

        # write the avro message to a test topic
        stream = hop.Stream(auth=self.auth)
        with stream.open(
            f"kafka://{self.scimma_hostname}/{self.rubin_topic}", "w"
        ) as s:
            print("writing avro message\n")
            s.write(hop.models.AvroBlob([message.content], schema=avro_schema))

    def _message_parsing(self, message, avro_schema=None):
        """Placeholder function for processing a message.

        Parameters
        ----------
        message : `hop.models.*`
            hop message model object received from a hop stream object.
        """
        # print("processing message: ", message)
        avro_schema = make_avro_schema_heartbeat(message)

        # write the avro message to a test topic
        stream = hop.Stream(auth=self.auth)
        with stream.open(
            f"kafka://{self.scimma_hostname}/{self.rubin_topic}", "w"
        ) as s:
            print("writing avro message\n")
            s.write(hop.models.AvroBlob([message.content], schema=avro_schema))

    def get_earliest_offset(self):
        """Shorthand function to access the earliest kafka offset. Useful when
        wishing to stream all messages in a topic (previous and new).
        """

        return hop.io.consumer.ConsumerStartPosition.EARLIEST

    def get_latest_offset(self):
        """Shorthand function to access the latest kafka offset. Useful when
        wishing to stream only new messages sent to a topic.
        """
        return hop.io.consumer.ConsumerStartPosition.LATEST

    async def start(self):
        """Make SCiMMA topic and associated topic producer."""

        if self.kafka_producer is not None:
            raise RuntimeError("Kafka producer already started.")
        self.kafka_producer = await self.kafka_factory.make_producer(
            avro_schema=self.avro_schema
        )
        self.done_task = asyncio.Future()

        event_loop = asyncio.get_event_loop()

        self._read_topic_task = event_loop.run_in_executor(None, self._read_topic)

    async def close(self):
        """Stop background tasks."""
        if not self._read_topic_task.done():
            self.log.info("Read topic task still running, cancelling.")
            self._read_topic_task.cancel()
            try:
                await self._read_topic_task
            except asyncio.CancelledError:
                pass
            except Exception:
                self.log.exception(
                    "Unexpected error cancelling read topic task. Ignoring."
                )

        if not self.done_task.done():
            self.log.warning("Done task not set.")
            self.done_task.set_result(False)

    async def done(self):
        """Wait until done_task finishes."""
        await self.done_task

    @classmethod
    async def amain(cls):
        """Parse command line arguments, create and run a `HopProducer`."""
        parser = cls.make_argument_parser()
        args = parser.parse_args()

        # Parse Kafka configuration, but first fix the type of wait_for_ack:
        # cast the value to int, unless it is "all".
        if args.broker_url is None or args.registry_url is None:
            parser.error("You must specify --broker and --registry.")
        if int(args.partitions) <= 0:
            parser.error(f"--partitions={args.partitions} must be positive")
        if args.wait_for_ack != "all":
            args.wait_for_ack = int(args.wait_for_ack)
        if args.username and args.password is None:
            parser.error("You must specify --password if you specify --username.")
        if args.password and args.username is None:
            parser.error("You must specify --username if you specify --password.")

        kafka_config = KafkaConfiguration(
            broker_url=args.broker_url,
            sasl_plain_username=args.username,
            sasl_plain_password=args.password,
            registry_url=args.registry_url,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            wait_for_ack=args.wait_for_ack,
        )

        log = logging.getLogger()
        if not log.hasHandlers():
            log.addHandler(logging.StreamHandler())
        log.setLevel(args.log_level)
        
        async with KafkaProducerFactory(
            config=kafka_config, log=log
        ) as kafka_producer_factory, cls(
            kafka_factory=kafka_producer_factory, log=log
        ) as hop_producer:
            await hop_producer.done()

    @staticmethod
    def make_argument_parser():
        """Make a command-line argument parser."""
        parser = argparse.ArgumentParser(description="Send SCiMMA messages to Kafka")
        parser.add_argument(
            "--broker",
            dest="broker_url",
            help="Kafka broker URL, without the transport. "
            "Example 'my.kafka:9000'. "
            "Required unless --validate or --show-schema are specified.",
        )
        parser.add_argument(
            "--username",
            default=None,
            dest="username",
            help="Kafka username for SASL authentication. "
            "Required if --password is specified.",
        )
        parser.add_argument(
            "--scimma-hostname",
            default=None,
            dest="scimma_hostname",
            help="Hopskotch hostname to subscribe to. Production instance is"
            "kafka.scimma.org; development instance is dev.hop.scimma.org",
        )
        parser.add_argument(
            "--password",
            default=None,
            dest="password",
            help="Kafka password for SASL authentication. "
            "Required if --username is specified.",
        )
        parser.add_argument(
            "--registry",
            dest="registry_url",
            help="Schema Registry URL, including the transport. "
            "Example: 'https://registry.my.kafka/'. "
            "Required unless --validate or --show-schema are specified. ",
        )
        parser.add_argument(
            "--partitions",
            type=int,
            default=1,
            help="Number of partitions for each Kafka topic. "
            "A file specified as the --file argument may override this value "
            "for each set of topics.",
        )
        parser.add_argument(
            "--replication-factor",
            type=int,
            default=3,
            dest="replication_factor",
            help="Number of replicas for each Kafka partition.",
        )
        parser.add_argument(
            "--loglevel",
            type=int,
            dest="log_level",
            default=logging.INFO,
            help="Logging level; INFO=20 (default), DEBUG=10",
        )
        parser.add_argument(
            "--wait-ack",
            choices=("0", "1", "all"),
            default=1,
            dest="wait_for_ack",
            help="0: do not wait for ack from any Kafka broker (unsafe). "
            "1: wait for ack from one Kafka broker (default). "
            "all: wait for ack from all Kafka brokers.",
        )

        return parser

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, type, value, traceback):
        await self.close()


def run_hop_producer():
    asyncio.run(HopProducer.amain())
