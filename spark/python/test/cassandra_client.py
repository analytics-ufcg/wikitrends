#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
import logging

log = logging.getLogger()
log.setLevel('INFO')

class SimpleClient:
    session = None

    def connect(self, nodes):
        cluster = Cluster(nodes)
        metadata = cluster.metadata
        self.session = cluster.connect()
        log.info('Connected to cluster: ' + metadata.cluster_name)
        for host in metadata.all_hosts():
            log.info('Datacenter: %s; Host: %s; Rack: %s',
                host.datacenter, host.address, host.rack)

    def close(self):
        self.session.cluster.shutdown()
        self.session.shutdown()
        log.info('Connection closed.')


    def create_schema(self):
        self.session.execute("""CREATE KEYSPACE IF NOT EXISTS top_editors_batch_view WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};""")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS top_editors_batch_view.top_editors (
                login text,
                date date,
                event_time timestamp,
                data map<text,int>,
                
                PRIMARY KEY((login, date), event_time),
            ) WITH CLUSTERING ORDER BY (event_time DESC);
        """)
        log.info('top_editors_batch_view keyspace and schema created.')


    def load_data(self):
        self.session.execute("""
            INSERT INTO top_editors_batch_view.top_editors (login, date, event_time, data)
            VALUES (
                'john_123',
                '2013-10-21',
                1382313600,
                {'0': 2, '1': 0, '2': 4, '3': 3, '4': 10}
            );
        """)
        self.session.execute("""
            INSERT INTO top_editors_batch_view.top_editors (login, date, event_time, data)
            VALUES (
                'mary_123',
                '2013-10-10',
                1381363200,
                {'0': 2, '1': 0, '2': 2, '3': 3, '4': 3}
            );
        """)
        self.session.execute("""
            INSERT INTO top_editors_batch_view.top_editors (login, date, event_time, data)
            VALUES (
                'cara_123',
                '2013-10-13',
                1381622400,
                {'0': 2, '1': 0, '2': 34, '3': 3, '4': 10}
            );
        """)
        self.session.execute("""
            INSERT INTO top_editors_batch_view.top_editors (login, date, event_time, data)
            VALUES (
                'drew_123',
                '2013-10-14',
                1381708800,
                {'0': 34, '1': 12, '2': 4, '3': 3, '4': 4}
            );
        """)
        
    
    def query_schema(self):
        results = self.session.execute("""
            SELECT * FROM top_editors_batch_view.top_editors;
        """)
        print "%-20s\t%-20s\t%-20s\t%-20s" % ("login", "date", "event_time","data")
        print "-------------------------------+-----------------------+--------------------+-------------------------------"
        for row in results:
            print "%-20s\t%-20s\t%-20s\t%-20s" % (row.login, row.date, row.event_time, str(row.data))
        log.info('Schema queried.')

def main():
    logging.basicConfig()
    client = SimpleClient()
    client.connect(['127.0.0.1'])
    client.create_schema()
    #client.load_data()
    #client.query_schema()
    client.close()

if __name__ == "__main__":
    main()