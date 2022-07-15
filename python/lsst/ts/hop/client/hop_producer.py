import hop


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

    def __init__(self, hostname="kafka.scimma.org", auth="current"):
        self.hostname = hostname
        if auth == "current":
            self.auth = hop.auth.select_matching_auth(hop.auth.load_auth(), hostname)

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
        with stream.open(f"kafka://{self.hostname}/{topic}", "r") as s:
            for message in s:
                self._message_parsing(message)

    def _message_parsing(self, message):
        """Placeholder function for processing a message. Currently just print
        the message.

        Parameters:
        message: `hop.models.*`
            hop message model object received from a hop stream object.
        """
        print(message)

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

    test_producer = hop_producer(scimma_hostname)
    test_producer._read_topic()
