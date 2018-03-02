#!/bin/python

import argparse
import sys

from app.clients.mysql import MysqlClient
from app.subscription_manager import SubscriptionManager
from app.util.logger import Logger
from app.exchanges.gdax_order_book import GDaxOrderBook
from app.actors.graphing_actor import GraphingActor
from collections import defaultdict
import time


def main():
    suported_books = {
        'gdax': GDaxOrderBook
    }
    parser = argparse.ArgumentParser(description='Crypto exchange data handler.')
    parser.add_argument('-instmts', action='store', help='Instrument subscription file.', default='subscriptions.ini')
    parser.add_argument('-mysql', action='store_true', help='Use MySQL.')
    parser.add_argument('-mysqldest', action='store', dest='mysqldest',
                        help='MySQL destination. Formatted as <name:pwd@host:port>',
                        default='')
    parser.add_argument('-mysqlschema', action='store', dest='mysqlschema',
                        help='MySQL schema.',
                        default='')
    parser.add_argument('-output', action='store', dest='output',
                        help='Verbose output file path')
    args = parser.parse_args()

    Logger.init_log(args.output)

    db_clients = []
    is_database_defined = False
    if args.mysql:
        db_client = MysqlClient()
        mysqldest = args.mysqldest
        user = mysqldest.split('@')[0].split(':')[0]
        pwd = mysqldest.split('@')[0].split(':')[1]
        host = mysqldest.split('@')[1].split(':')[0]
        port = int(mysqldest.split('@')[1].split(':')[1])
        db_client.connect(host=host,
                          port=port,
                          user=user,
                          pwd=pwd,
                          schema=args.mysqlschema)
        db_clients.append(db_client)
        is_database_defined = True

    if not is_database_defined:
        print('Error: Please define which database is used.')
        parser.print_help()
        sys.exit(1)

    # Subscription instruments
    if args.instmts is None or len(args.instmts) == 0:
        print('Error: Please define the instrument subscription list. You can refer to subscriptions.ini.')
        parser.print_help()
        sys.exit(1)

    # Initialize subscriptions
    subscription_instmts = SubscriptionManager(args.instmts).get_subscriptions()
    if len(subscription_instmts) == 0:
        print('Error: No instrument is found in the subscription file. ' +
              'Please check the file path and the content of the subscription file.')
        parser.print_help()
        sys.exit(1)

    Logger.info('[main]', 'Subscription file = %s' % args.instmts)
    log_str = 'Exchange/Instrument/InstrumentCode:\n'
    for instmt in subscription_instmts:
        log_str += '%s/%s/%s\n' % (instmt.exchange_name, instmt.instmt_name, instmt.instmt_code)
    Logger.info('[main]', log_str)

    actors = [GraphingActor()]
    actor_refs = []
    for actor in actors:
        actor_refs.append(actor.start())

    subs = defaultdict(list)
    for instmt in subscription_instmts:
        Logger.info("[main]",
                    "Starting instrument {}-{}...".format(instmt.get_exchange_name(), instmt.get_instmt_name()))
        subs[instmt.get_exchange_name().lower()].append(instmt.get_instmt_code())

    started_exchanges = []
    for exchange, products in subs.items():
        book = suported_books[exchange]
        if book:
            book_to_start = book(
                actors=actor_refs,
                products=products)
            book_to_start.start()
            started_exchanges.append(book_to_start)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        for actor in actor_refs:
            actor.stop()
        for exch in started_exchanges:
            exch.close()


if __name__ == '__main__':
    main()
