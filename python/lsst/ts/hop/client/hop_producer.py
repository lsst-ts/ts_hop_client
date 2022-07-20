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

import hop

import fastavro

from make_avro_schema import *


class hopProducer:
    """Produce kafka messages from SCiMMA Hopskotch kafka topics.

    Parameters
    hostname: `str`
        Hopskotch hostname to subscribe to. Production instance is
        kafka.scimma.org; development instance is dev.hop.scimma.org
    auth:    `hop.Auth` or `False`
        auth object to use to authenticate to a SCiMMA topic. If False, no
        authentication is attempted. Default is to use any existing credentials
        associated with the current hop-client installation.
    """

    def __init__(self,
                 rubin_topic="rubin.testing-schema",
                 scimma_hostname="kafka.scimma.org",
                 auth="current"):
        self.rubin_topic = rubin_topic
        self.scimma_hostname = scimma_hostname
        if auth == "current":
            self.auth = hop.auth.select_matching_auth(hop.auth.load_auth(),
                                                      scimma_hostname)
        self.topic_schema = None

    def _read_topic(
        self,
        topic="sys.heartbeat",
        start_offset=hop.io.consumer.ConsumerStartPosition.LATEST,
    ):
        """Start streaming messages from a kafka topic.

        Parameters:
        topic: `str`
            kafka topic from which to stream. Defaults to Hopskotch system
            heartbeat.
        start_offset: `hop.io.consumer.ConsumerStartPosition`
            kafka topic offset from which to start the stream at (EARLIEST or
            LATEST). Defaults to the latest offest, so only new messages are
            streamed.

        """

        stream = hop.Stream(auth=self.auth, start_at=start_offset)
        with stream.open(f"kafka://{self.scimma_hostname}/{topic}", "r") as s:
            for message in s:
                self._message_parsing(message)

    def _message_printing(self, message):
        """Placeholder function for processing a message. Currently just print
        the message.

        Parameters:
        message: `hop.models.*`
            hop message model object received from a hop stream object.
        """
        print(message)

    def _message_parsing(self, message):
        """Placeholder function for processing a message.

        Parameters:
        message: `hop.models.*`
            hop message model object received from a hop stream object.
        """
        # print("processing message: ", message)

        avro_message_schema = make_avro_schema_heartbeat(message)

        # write the avro message to a test topic
        stream = hop.Stream(auth=self.auth)
        with stream.open(f"kafka://{self.scimma_hostname}/{self.rubin_topic}",
                         "w") as s:
            print("writing avro message\n")
            s.write(hop.models.AvroBlob([message.content],
                                        schema=avro_message_schema))
        
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


if __name__ == "__main__":
    scimma_hostname = "kafka.scimma.org"
    rubin_topic = "rubin.testing-schema"

    test_producer = hopProducer(rubin_topic, scimma_hostname)
    test_producer._read_topic()
