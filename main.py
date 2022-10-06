#!/usr/bin/env python3

import argparse
from getpass import getpass
from vm_vcenter import VMvCenter

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data and IP scraper from VMware vCenter. Supports output to Kafka, Elasticsearch, file or stdout. \nNote, current implementation selects the first "Datacenter" entity in the vCenter.')
    # vSphere args
    parser.add_argument('-vh', '--vhost',
                        required=True,
                        action='store',
                        help='vSphere service address to connect to')
    parser.add_argument('-vp', '--vport',
                        type=int,
                        default=443,
                        action='store',
                        help='vSphere port to connect on. Default: 443')
    parser.add_argument('-u', '--user',
                        required=True,
                        action='store',
                        help='User name to use when connecting to vSphere')
    parser.add_argument('-p', '--password',
                        required=False,
                        action='store',
                        help='Password to use when connecting to vSphere. Prompts for password if not provided.')
    parser.add_argument('-nossl', '--disable-ssl-verification',
                        required=False,
                        action='store_true',
                        help='Disable ssl host certificate verification')
    parser.add_argument('-f', '--folder',
                        required=True,
                        action='store',
                        help='Path to the VM folder to scrape (recursively). Do not prefix the path with /.')
    # Elasticsearch args
    parser.add_argument('-eh', '--es-host',
                        dest='es_host', 
                        action='store',
                        help='Elasticsearch service address to connect to')
    parser.add_argument('-i', '--index',
                        dest='index',
                        action='store',
                        default='vmware-assets', 
                        help='Elasticsearch index to push data on')
    # Kafka args
    parser.add_argument('--kafka-topic', 
                        dest='kafka_topic', 
                        default='vmware-assets', 
                        help='Kafka topic to publish messages to. Default: xs-vmware-assets')
    parser.add_argument('--kafka-brokers', '--kafka-hosts',
                        dest='kafka_host', 
                        nargs = '+', 
                        help='Kafka broker(s). Separate multiple brokers with a whitespace.')
    parser.add_argument('--kafka-compression', 
                        dest='kafka_compression', 
                        default='gzip',
                        choices=['gzip', 'lz4', 'snappy'],
                        help='Compression method to use. Use one of: gzip, lz4 (pip3 install lz4), snappy (pip3 install python-snappy). Default: gzip')
    parser.add_argument('--kafka-ip-split',
                        dest='ip_split',
                        action='store_true',
                        default=False, 
                        help='Splits each found IP[4|6] per one line published to Kafka')
    # File store args
    parser.add_argument('-o', '--output', '--file', 
                        dest='file_path', 
                        help='File path to dump the collected data.')
    parser.add_argument('--file-format', 
                        dest='file_format', 
                        default='json',
                        choices=['json', 'yaml'],
                        help='File format for the dumpfile. Default: json')
    # Other args
    parser.add_argument('-v', '--verbose',
                        dest='verbose',
                        action='store_true',
                        default=False, 
                        help='Increases verbosity. Produces information messages about the progress.')


    # Get args
    args = parser.parse_args()
    es_enabled = True if args.es_host else False
    kafka_enabled = True if args.kafka_host else False
    file_enabled = True if args.file_path else False
    stdout_enabled = True if not (es_enabled or kafka_enabled or file_enabled) else False

    # Conditional imports to avoid unnecessary dependency requirements
    if es_enabled:
        from elastic_link import ElasticLink
    if kafka_enabled:
        from kafka_link import KafkaLink
    if file_enabled:
        from file_link import FileLink
    if stdout_enabled:
        import pprint
        pp = pprint.PrettyPrinter(indent=2)


    # Check password
    if not args.password:
        args.password = getpass(
            prompt='Please enter password for host %s and user %s: '
                   % (args.vhost, args.user))

    # Init vCenter obj
    vm_vcenter = VMvCenter(host=args.vhost,
                           user=args.user,
                           password=args.password,
                           port=args.vport,
                           disable_ssl_verification=args.disable_ssl_verification,
                           folder_path=args.folder)

    # Init ElasticLink obj
    if es_enabled:
        if args.verbose:
            print('Connecting to ES host:' + str(args.es_host))
        ela_link = ElasticLink(ela_host=args.es_host,
                               ela_index=args.index)
        ela_link.connect()

    # Init KafkaLink obj
    if kafka_enabled:
        if args.verbose:
            print('Connecting to Kafka hosts: ' + str(args.kafka_host))

        kafka_link = KafkaLink(kafka_host=args.kafka_host,
                            kafka_topic=args.kafka_topic,
                            kafka_compression=args.kafka_compression,
                            ip_split=args.ip_split)
        kafka_link.connect()
    
    # Init FileLink obj
    if file_enabled:
        if args.verbose:
            print('Opening ' + args.file_path + ' for writing.')
        file_link = FileLink(file_path=args.file_path, file_format=args.file_format)

    # Initiate connection
    try:
        vm_vcenter.connect()
        if args.verbose:
            print('Connected to ' + str(args.vhost))
    except Exception as e:
        print('Unable to connect: ', e)
        exit(1)

    # Going through each VM
    counter = 0
    for vm in vm_vcenter.get_vm_iterator_from_folder():
        counter += 1
        try:
            vm_info = vm_vcenter.get_vm_info(vm)
            if es_enabled:
                ela_link.push_to_server(vm_info)
            if kafka_enabled:
                kafka_link.push_to_server(vm_info)
            if file_enabled:
                file_link.write(vm_info)
            if stdout_enabled:
                pp.pprint(vm_info)

        except Exception as e:
            print('Error occurred: ', e)

    if args.verbose:
        print('Done! Collected info from ' + str(counter) + ' hosts.')
