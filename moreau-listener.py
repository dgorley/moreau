#!/usr/bin/env python
#-*- coding: utf-8 -*-

"""
moreau-listener.py - Bridge PostgreSQL NOTIFYs to RabbitMQ.

Copyright (c) 2014, Doug Gorley.  All rights reserved.
This project is distributed under the BSD license; see LICENSE.txt.
"""

import sys
import select
import pika
import psycopg2
import psycopg2.extensions
import argparse
import glob
import itertools
import configparser as configparser
from collections import OrderedDict
from multiprocessing import Process

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(processName)s %(message)s')


def show_header():
    """Banner displayed on startup."""
    print("""
##############################################################

  ##     ##  #######  ########  ########    ###    ##     ##
  ###   ### ##     ## ##     ## ##         ## ##   ##     ##
  #### #### ##     ## ##     ## ##        ##   ##  ##     ##
  ## ### ## ##     ## ########  ######   ##     ## ##     ##
  ##     ## ##     ## ##   ##   ##       ######### ##     ##
  ##     ## ##     ## ##    ##  ##       ##     ## ##     ##
  ##     ##  #######  ##     ## ######## ##     ##  #######

##############################################################

  Copyright (c) 2014, Doug Gorley
  All rights reserved.

  Copyright (c) 2014, Doug Gorley.  All rights reserved.
  This project is distributed under the BSD license;
  see LICENSE.txt for details.

###############################################################
""")

def parse_cmdline():
    """
    Parse options on the command line.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'config_file', metavar='FILE', nargs='+',
        help='A config file describing a messaging bridge.')
    parser.add_argument(
        '-p', '--persistent', action='store_true',
        help='Maintain a constant connection to RabbitMQ. (default=false)')
    args = parser.parse_args()
    return args


def config_list(args):
    """
    Produce a list of config files derived from the command
    line options.  This allows for wildcards (as per glob.)
    """
    return list(itertools.chain(*[glob.glob(n) for n in args.config_file]))


def parse_config(filename):
    """
    Parse the config file.
    """
    logging.debug('Parsing config file %s.' % (filename,))
    config = configparser.SafeConfigParser()
    config.read(filename)
    checks = (
        ('bridge', 'name'),
        ('rabbitmq', 'host'),
        ('rabbitmq', 'port'),
        ('rabbitmq', 'vhost'),
        ('rabbitmq', 'exchange'),
        ('rabbitmq', 'exchange_type'),
        ('rabbitmq', 'username'),
        ('rabbitmq', 'password'),
        ('postgres', 'host'),
        ('postgres', 'port'),
        ('postgres', 'database'),
        ('postgres', 'username'),
        ('postgres', 'password'),
        ('postgres', 'channel'))
    for check_pair in checks:
        if not config.has_option(*check_pair):
            logging.critical('Missing configuration option %s:%s.', *check_pair)
            logging.critical('Unable to continue; exiting.')
            sys.exit(1)
    config_dict = OrderedDict()
    for section in config.sections():
        config_dict[section] = OrderedDict()
        for check_pair in config.options(section):
            config_dict[section][check_pair] = config.get(section, check_pair)
    return config_dict


def connect_rabbitmq(config):
    """
    Connect to the RabbitMQ broker.
    """
    logging.debug('Connecting to the RabbitMQ broker.')
    try:
        params = pika.ConnectionParameters(
            host=config['rabbitmq']['host'],
            port=int(config['rabbitmq']['port']),
            virtual_host=config['rabbitmq']['vhost'],
            credentials=pika.PlainCredentials(
                config['rabbitmq']['username'],
                config['rabbitmq']['password']))
        conn = pika.BlockingConnection(params)
    except:
        logging.critical('Could not connect to RabbitMQ broker.')
        logging.critical('Unable to continue; exiting.')
        sys.exit(1)
    logging.info('Connection to RabbitMQ established.')
    return conn


def connect_postgres(config):
    """
    Connect to the PostgreSQL server.
    """
    logging.debug('Connecting to the PostgreSQL server.')
    try:
        conn = psycopg2.connect(
            host=config['postgres']['host'],
            port=config['postgres']['port'],
            database=config['postgres']['database'],
            user=config['postgres']['username'],
            password=config['postgres']['password'])
        conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    except:
        logging.critical('Could not connect to PostgreSQL server.')
        logging.critical('Unable to continue; exiting.')
        sys.exit(1)
    logging.info('Connection to PostgreSQL established.')
    return conn


def register_listen(conn, config):
    """
    Start the PostgreSQL connection listening on a specified channel.
    """
    channel = config['postgres']['channel']
    logging.debug('Preparing to start listening on channel "%s".', channel)
    try:
        curs = conn.cursor()
        curs.execute('LISTEN {};'.format(channel))
    except:
        raise
        logging.critical('Could not listen on channel "%s".', channel)
        logging.critical('Unable to continue; exiting.')
        sys.exit(1)
    logging.info('Listening on channel "%s".', channel)


def poll(conn_postgres, conn_rabbitmq, config, callback):
    """
    Poll for messages to bridge.
    """
    logging.info('Beginning polling for messages.')
    while 1:
        if select.select([conn_postgres], [], [], 2) == ([], [], []):
            logging.debug('Timed out while polling. (this is normal)')
        else:
            conn_postgres.poll()
            while conn_postgres.notifies:
                notify = conn_postgres.notifies.pop()
                callback(conn_rabbitmq, config, notify.payload)

def republish_message(conn, config, pg_payload):
    """
    Parse the raw payload from the PostgreSQL NOTIFY, and republish
    it to the RabbitMQ broker.
    """
    if conn is None:
        terminate_after_message = True
        conn = connect_rabbitmq(config)
    else:
        terminate_after_message = False
    try:
        routing_key, message = pg_payload.split(':', 1)
    except:
        routing_key, message = None, None
    if not routing_key or not message:
        logging.warning('Improperly formatted message received; discarding.')
        logging.info('Message content: %s', pg_payload)
        return
    try:
        channel = conn.channel()
        if config['rabbitmq']['exchange']:
            channel.exchange_declare(
                exchange=config['rabbitmq']['exchange'],
                type=config['rabbitmq']['exchange_type'])
        if 'queue' in config['rabbitmq']:
            channel.queue_declare(queue=config['rabbitmq']['queue'])
        channel.basic_publish(
            exchange=config['rabbitmq']['exchange'],
            routing_key=routing_key,
            body=message)
        logging.info(
            'Message republished via "%s" bridge.',
            config['bridge']['name'])
        logging.debug('Routing key: %s', routing_key)
        logging.debug('Message: %s', message)
    except:
        logging.warning(
            'Unable to republish message via "%s" bridge; discarding.',
            config['bridge']['name'])
        logging.info('Message content: %s', pg_payload)
    if terminate_after_message:
        conn.close()


def run(args, config):
    """
    Begin listening on a specific bridge.
    """
    logging.info('Initiating bridge "%s".', config['bridge']['name'])
    if args.persistent:
        conn_rabbitmq = connect_rabbitmq(config)
    else:
        conn_rabbitmq = None
    conn_postgres = connect_postgres(config)
    register_listen(conn_postgres, config)
    poll(conn_postgres, conn_rabbitmq, config, republish_message)


if __name__ == '__main__':
    ARGS = parse_cmdline()
    show_header()

    MASTER_CONFIG = OrderedDict()
    for config_file in config_list(ARGS):
        this_config = parse_config(config_file)
        bridge_name = this_config['bridge']['name']
        MASTER_CONFIG[bridge_name] = this_config

    PROC_LIST = []
    for bridge in MASTER_CONFIG:
        proc = Process(
            target=run,
            name=bridge,
            args=(ARGS, MASTER_CONFIG[bridge],))
        proc.daemon = True
        proc.start()
        PROC_LIST.append(proc)
    for p in PROC_LIST:
        p.join()
