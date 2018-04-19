#!/usr/bin/python
"""
Returns a list of extents with their block statuses for an NBD export.

This program uses new NBD capabilities introduced in QEMU 2.12.

It uses the BLOCK_STATUS NBD extension, which is documented here:
https://github.com/NetworkBlockDevice/nbd/blob/extension-blockstatus/doc/proto.md
The structured replies functionality that this extension relies on is in the
main branch of the NBD protocol docs:
https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
"""

import argparse
import json
import logging
import logging.handlers

from python_nbd_client import PythonNbdClient
import python_nbd_client


LOGGER = logging.getLogger("get_nbd_extents")
LOGGER.setLevel(logging.DEBUG)

# Request length is a 32-bit int.
# It looks like for qemu 2.12, the length of a NBD_CMD_BLOCK_STATUS is not
# limited by the maximum block size of the server, only the size of the 32-bit
# int.
MAX_REQUEST_LEN = 2 ** 32 - 1

# Make the NBD_CMD_BLOCK_STATUS request aligned to 512 bytes, just in case. But
# it looks like this is not required either for qemu 2.12.
MAX_REQUEST_LEN = MAX_REQUEST_LEN - (MAX_REQUEST_LEN % 512)

def _get_extents(path, exportname):
    with PythonNbdClient(
        address=path, exportname=exportname, unix=True, use_tls=False, connect=False) as client:
        client.negotiate_structured_reply()

        context = 'base:allocation'
        selected_contexts = client.set_meta_contexts(exportname, [context])
        assert len(selected_contexts) == 1
        (meta_context_id, meta_context_name) = selected_contexts[0]
        assert meta_context_name == context

        client.connect(exportname)

        size = client.get_size()
        LOGGER.debug(
            'Connected to NBD export %s served at path %s of size %d bytes',
            exportname, path, size)

        offset = 0
        while offset < size:
            request_len = min(MAX_REQUEST_LEN, size - offset)
            replies = client.query_block_status(offset, request_len)

            # Process the returned structured reply chunks
            # "For a successful return, the server MUST use a structured reply,
            # containing exactly one chunk of type NBD_REPLY_TYPE_BLOCK_STATUS"
            assert len(replies) == 1
            reply = replies[0]

            # First make sure it's a block status reply
            if reply['reply_type'] & python_nbd_client.NBD_REPLY_TYPE_ERROR_BIT != 0:
                raise Exception('Received error: {}'.format(reply))
            if reply['reply_type'] != python_nbd_client.NBD_REPLY_TYPE_BLOCK_STATUS:
                raise Exception('Unexpected reply: {}'.format(reply))

            # Then process the returned block status info
            assert reply['context_id'] == meta_context_id
            descriptors = reply['descriptors']
            for descriptor in descriptors:
                (length, flags) = descriptor
                yield {'length':length, 'flags':flags}
                offset += length

if __name__ == '__main__':
    # Configure the root logger to log into syslog
    # (Specifically, into /var/log/user.log)
    SYSLOG_HANDLER = logging.handlers.SysLogHandler(
        address='/dev/log',
        facility=logging.handlers.SysLogHandler.LOG_USER)
    logging.getLogger().addHandler(SYSLOG_HANDLER)

    try:
        parser = argparse.ArgumentParser(
            description="Return a list of extents with their block statuses")
        parser.add_argument(
            '--path',
            required=True,
            help="The path of the Unix domain socket of the NBD server")
        parser.add_argument(
            '--exportname',
            required=True,
            help="The export name of the device to connect to")

        args = parser.parse_args()

        extents = list(_get_extents(path=args.path, exportname=args.exportname))
        print json.dumps(extents)
    except Exception as exc:
        LOGGER.exception(exc)
        raise
