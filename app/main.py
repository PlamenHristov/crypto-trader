#!/bin/python

import argparse
import sys
import time
from collections import defaultdict

from actors.graphing_actor import GraphingActor
from exchanges.bittrex_orderbook import BittrexOrderBook
from exchanges.gdax_orderbook import GDaxOrderBook
from subscription_manager import SubscriptionManager
from util.logger import Logger


def main():
    parser = argparse.ArgumentParser(description='Crypto exchange data handler.')
    parser.add_argument('-instmts', action='store', help='Instrument subscription file.', default='subscriptions.ini')
    parser.add_argument('-output', action='store', dest='output',
                        help='Verbose output file path')
    args = parser.parse_args()

    Logger.init_log(args.output)

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

    actors = [GraphingActor]
    suported_books = [GDaxOrderBook,BittrexOrderBook]
    actor_refs = []
    for actor in actors:
        actor_refs.append(actor.start())

    subs = defaultdict(list)
    for instmt in subscription_instmts:
        Logger.info("[main]",
                    "Starting instrument {}-{}...".format(instmt.get_exchange_name(), instmt.get_instmt_name()))
        subs[instmt.get_exchange_name().lower()].append(instmt.get_instmt_code())

    started_exchanges = []
    for book in suported_books:
        for exchange, products in subs.items():
            if book.exchange_name.lower() == exchange.lower():
                book_to_start = book(
                    actors=actor_refs,
                    products=products
                )
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
