"""

commands1:2017-10-26T14:51:21.792-0500 I COMMAND
[conn268] command colliderChecklists.itemRefs command: insert { insert: "itemRefs", ordered: true, documents: [ { id: "file:/786645f5-e2b1-49f0-9ede-c775f10bef19/r/eed8595e-da56-47a5-976f-6871b6815ebc/e/KS/f/entity.txt?rev=669ab88db0e1239336a21f54e36185a02f19c943", available: true, href: { value: "/786645f5-e2b1-49f0-9ede-c775f10bef19/r/eed8595e-da56-47a5-976f-6871b6815ebc/e/KS/f/entity.txt?rev=669ab88db0e1239336a21f54e36185a02f19c943", present: true }, type: "STORE", props: {}, colliderChecklistIds: [] } ] }
ninserted:1 keysInserted:2 numYields:0 reslen:84 locks:{ Global: { acquireCount: { r: 2, w: 2 } },
Database: { acquireCount: { w: 2 }, acquireWaitCount: { w: 1 }, timeAcquiringMicros: { w: 1244959 } },
Collection: { acquireCount: { w: 1 } }, Metadata: { acquireCount: { w: 1 } }, oplog: { acquireCount: { w: 1 } } } protocol:op_query 1245ms

"""
import argparse
from sys import argv


def get_op_acqu(file_name, max_acq_ratio):
    line_count = 0
    cmd_count = 0
    acq_lines = 0
    with open(file_name,'rt') as f:

        for line in f:

            line = line.rstrip()
            tokens = line.split(' ')

            if tokens[2] == 'COMMAND':

                if line.find('timeAcquiringMicros') > -1:
                    acq_lines += 1
                    x = 0
                    for i in xrange(0, len(tokens)):
                        if tokens[i] == 'timeAcquiringMicros:':
                            x = float(tokens[i+3])
                            break

                    op_time = tokens[-1]
                    acq_ratio = (x / (1000.0 * float(op_time[:-2]))) * 100

                    if acq_ratio > max_acq_ratio:
                        print line,
                        acq_time = line[line.find('timeAcquiringMicros'):]
                        #print "\t%s" % (acq_time)
                        print "-> Acquiring percentage of time %f" % acq_ratio
                else:
                    #print tokens[0], tokens[5], tokens[6], tokens[7], tokens[8]
                    op_query = line[line.find('protocol:op_query'):]
                    #print "\t%s" % (op_query)


                cmd_count += 1
            line_count += 1


    print "Lines %d, Command lines %d, aquisition %d" % (cmd_count, line_count, acq_lines)


if __name__ == "__main__":
    file_name = '/Users/jorgeimperial/Downloads/TICKETS/462095/mongod.log.2017-10-26T19-30-50'
    file_name = '/Users/jorgeimperial/Downloads/TICKETS/462095/mongod.log.2017-10-26T16-00-13'
    file_name = '/Users/jorgeimperial/Downloads/TICKETS/462095/mongod.log-20171027_05'

    parser = argparse.ArgumentParser(description='Extracts commands from a log where the acquisition time is more than a given percentage.')

    parser.add_argument('-f', '--file',
                        help='Path to mongod.log where commands are to be extracted. Default is mongod.log',
                        default='mongod.log')
    parser.add_argument('-p', '--percentage',
                        help='Minimum percentage for commands to be extracted. Default is 33.0',
                        type=float,
                        default=33.0)

    config = parser.parse_args(argv[1:])

    get_op_acqu(config.file, float(config.percentage))