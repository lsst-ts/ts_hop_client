# This file is part of ts_salkafka.
#
# Developed for the LSST Telescope and Site Systems.
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

# adapted for use with SCiMMA messages from
# https://github.com/lsst-ts/ts_salkafka

__all__ = [
    "make_avro_schema_heartbeat",
    "make_avro_schema_heartbeat_message",
]

from lsst.ts import utils

_SCALAR_TYPE_DICT = {
    bool: "boolean",
    int: "long",
    float: "double",
    str: "string",
}


def make_avro_schema_test():
    """Make an Avro schema for the SCiMMA rubin.testing-alert topic.

    Returns
    -------
    avro_schema : `dict`
        the Avro schema for the testing-alert topic
    """

    fields = [
        dict(
            name="private_efdStamp",
            type="double",
            description="UTC time for EFD timestamp. "
            "An integer (the number of leap seconds) "
            "different from private_sndStamp.",
            units="second",
            default=0,
        ),
        dict(
            name="private_kafkaStamp",
            type="double",
            description="TAI time at which the Kafka message was created.",
            units="second",
            default=0,
        ),
        dict(
            name="name", type="string", description="message name", default="message",
        ),
        dict(
            name="description",
            type="string",
            description="message description",
            default="default message description",
        ),
    ]

    avro_schema = dict(
        name="lsst.sal.scimma.rubin.testing_alert", type="record", fields=fields,
    )

    return avro_schema


def make_avro_schema_heartbeat():
    """Make an Avro schema for the SCiMMA heartbeat topic.

    Returns
    -------
    avro_schema : `dict`
        the Avro schema for the heartbeat topic
    """

    fields = [
        dict(
            name="private_efdStamp",
            type="double",
            description="UTC time for EFD timestamp. "
            "An integer (the number of leap seconds) "
            "different from private_sndStamp.",
            units="second",
            default=0,
        ),
        dict(
            name="private_kafkaStamp",
            type="double",
            description="TAI time at which the Kafka message was created.",
            units="second",
            default=0,
        ),
        dict(
            name="timestamp", type="long", description="message timestamp", default=0,
        ),
        dict(name="count", type="long", description="heartbeat count", default=0,),
        dict(
            name="beat",
            type="string",
            description="message content",
            default="default beat content",
        ),
    ]

    avro_schema = dict(
        name="lsst.sal.scimma.sys_heartbeat", type="record", fields=fields,
    )

    return avro_schema


def make_avro_schema_heartbeat_message(message):
    """Make an Avro schema for a given heartbeat message.

    Parameters
    ----------
    message : `hop.models.*`
        message for which to generate the schema.

    Returns
    -------
    `dict`
        Dictionary with the data from the input message.
    """
    current_tai = utils.current_tai()
    avro_data = dict(message.content.items())
    avro_data["private_efdStamp"] = utils.utc_from_tai_unix(current_tai)
    avro_data["private_kafkaStamp"] = current_tai

    return avro_data
