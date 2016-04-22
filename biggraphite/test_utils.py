#!/usr/bin/env python
from __future__ import print_function

from cassandra import cluster as c_cluster


def create_unreplicated_keyspace(contact_points, port, keyspace):
    """Creates a keyspace, mostly used for tests."""
    cluster = c_cluster.Cluster(contact_points, port)
    session = cluster.connect()
    session.execute(
        "CREATE KEYSPACE %s "
        " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};" %
        keyspace)
    session.shutdown()
    cluster.shutdown()
