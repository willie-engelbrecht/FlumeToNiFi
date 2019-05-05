#!/usr/bin/env python3.4

import pprint
import datetime
import random
import nipyapi
import sys
from optparse import OptionParser

optp = OptionParser()
optp.add_option("-n", "--nifi-host", dest="nifihost",     help="NiFi Hostname URL Including port. Eg: http://localhost:9090/nifi-api")
optp.add_option("-f", "--filename",  dest="filename",     help="Input Flume file to process")
opts, args = optp.parse_args()

nifi_host = 'http://localhost:9090/nifi-api'
flume_config = ''

if opts.nifihost:
    nifi_host  = opts.nifihost
if opts.filename:
    flume_config  = opts.filename

if not nifi_host:
    print("Must supply NiFi host name")
    sys.exit()
if not flume_config:
    print("Must supply Flume Config File")
    sys.exit()

sources = {}
channels = {}
sinks = {}

f = open(flume_config, "r")
alllines = ''
for line in f: # Read line by line
    alllines += line
    if not line.startswith("#") and not len(line.strip()) == 0: # Strip out empty lines and starting with #

        input_split = line.strip().split('.',3) # Split on the dots
        d_name, keys = input_split[0], input_split[1:]
        #print(str(keys))

        # For lines with 3 dot-pairs or more
        if len(keys) == 3:
            input_split2 = keys[-1].strip().split('=',1) # Split the last two dot-pairs into separate keys

            # Now build/update the dictionaries
            # Handle Sources
            if keys[0] == 'sources':
                if not sources.get(keys[1]):
                    sources.update({keys[1]:{}})
                sources[keys[1]].update({str(input_split2[0]).strip():str(input_split2[1]).strip()})

            # Handle Sinks
            if keys[0] == 'sinks':
                if not sinks.get(keys[1]):
                    sinks.update({keys[1]:{}})
                sinks[keys[1]].update({str(input_split2[0]).strip():str(input_split2[1]).strip()})

            # Handle Channels
            if keys[0] == 'channels':
                if not channels.get(keys[1]):
                    channels.update({keys[1]:{}})
                channels[keys[1]].update({str(input_split2[0]).strip():str(input_split2[1]).strip()})

# Pretty Print the output of Sources, Sinks, Channels
pp = pprint.PrettyPrinter(depth=4)
print("Sources: ")
pp.pprint(sources)
print("\nSinks: ")
pp.pprint(sinks)
print("\nChannels: ")
pp.pprint(channels)

# Do NiFi Stuff:
nipyapi.config.nifi_config.host = nifi_host

# Create a new ProcessGroup
rootPgId = nipyapi.canvas.get_process_group_status(nipyapi.canvas.get_root_pg_id(), 'all')
CustomPgId = nipyapi.canvas.create_process_group(rootPgId, 'Imported Flume: ' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), (int(-500+random.randint(1,401)),int(-500+random.randint(1,401))), alllines)

print("\n\n")
sinkcount = len(sinks)
# Parse Source, Channel, Sink and create a NiFi flow matching the logic
# We start with Source...
source_proc = None
for s in sources:
    print("Source type: " + str(sources[s]['type']))
    print("Source location: " + str(float((sinkcount*500)+(((sinkcount-1)*500)/2))))
    if sources[s]['type'] == 'exec': # ExecuteProcess
        input_split = sources[s]['command'].strip().split(' ',1)
        source_proc = nipyapi.canvas.create_processor(parent_pg=CustomPgId, 
                                                      processor=nipyapi.canvas.get_processor_type('ExecuteProcess'), 
                                                      location=((sinkcount*500)+(((sinkcount-1)*500)/2),0), 
                                                      name='Flume (Source): ' + str(sources[s]['type']),
                                                      config=nipyapi.nifi.ProcessorConfigDTO(
                                                          properties={"Command" : input_split[0],
                                                                      "Command Arguments" : input_split[1],
                                                                      "Batch Duration" : "5s"},
                                                          auto_terminated_relationships=['success']
                                                      ))
#        print(str(source_proc))
        
# Now do Sinks
sinkcount = sinkcount+1 if sinkcount > 1 else sinkcount
for s in sinks:
    print("Sink type: " + str(sinks[s]['type']))
    print("Sink location: " + str(float((sinkcount*500))))
    if sinks[s]['type'] == 'hdfs': # PutHDFS
        sink_proc = nipyapi.canvas.create_processor(parent_pg=CustomPgId, 
                                                      processor=nipyapi.canvas.get_processor_type('PutHDFS'), 
                                                      location=(sinkcount*500,300), 
                                                      name='Flume (Sink): ' + str(sinks[s]['type']),
                                                      config=nipyapi.nifi.ProcessorConfigDTO(
                                                          properties={"Directory" : sinks[s]['hdfs.path']},
                                                          auto_terminated_relationships=['success','failure']
                                                      ))
     
    if sinks[s]['type'] == 'logger': # LogMessage
        sink_proc = nipyapi.canvas.create_processor(parent_pg=CustomPgId, 
                                                      processor=nipyapi.canvas.get_processor_type('LogMessage'), 
                                                      location=(sinkcount*500,300), 
                                                      name='Flume (Sink): ' + str(sinks[s]['type']),
                                                      config=nipyapi.nifi.ProcessorConfigDTO(
                                                          auto_terminated_relationships=['success','failure']
                                                      ))

    if sinks[s]['type'] == 'org.apache.flume.sink.kafka.KafkaSink': # PublishKafka_2_0
        sink_proc = nipyapi.canvas.create_processor(parent_pg=CustomPgId, 
                                                      processor=nipyapi.canvas.get_processor_type('PublishKafka_2_0'), 
                                                      location=(sinkcount*500,300), 
                                                      name='Flume (Sink): ' + 'kafka',
                                                      config=nipyapi.nifi.ProcessorConfigDTO(
                                                          properties={"bootstrap.servers" : sinks[s]['brokerList'],
                                                                      "topic" : sinks[s]['topic'],
                                                                      "acks" : 'all' },
                                                          auto_terminated_relationships=['success','failure']
                                                      ))
    sinkcount -= 1
#        print(str(sink_proc))
    # Link the two together through the Success relationship
    nipyapi.canvas.create_connection(source_proc, sink_proc)
        
