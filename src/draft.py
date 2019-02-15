from __future__ import absolute_import, division, print_function
from itertools import izip as zip
from datetime import datetime
from collections import OrderedDict

infile1 = '/Users/xinyuxu/PycharmProjects/edgar-analytics/insight_testsuite/tests/test_1/input/log.csv'
infile2 = '/Users/xinyuxu/PycharmProjects/edgar-analytics/insight_testsuite/tests/test_1/input/inactivity_period.txt'
outfile = '/Users/xinyuxu/PycharmProjects/edgar-analytics/output/sessionization.txt'
with open(infile2 , 'rb') as f:
    for line in f:
        period = line.strip()
inactivity_period = int(period)
log_csv_path = infile1

def read_input(log_csv_path):

    """Read input file 'log.csv' and get necessary columns.
    """

    log_file = []
    idx = 0
    col_names = ['ip' , 'date' , 'time']
    with open(log_csv_path, 'rb') as logs:
        for line in logs:
            if idx == 0:
                line = line.strip().split(",")
                header = line
                idx +=1
                col_index = [header.index(col_name) for col_name in col_names]
                cols_dict = {k: v for k, v in zip(col_names, col_index)}

            else:
                line = line.strip().split(",")
                line = [line[i] for i in col_index]
                log_file.append(line)
                idx += 1
    return log_file, cols_dict, idx-1

log_file, cols_dict, idx = read_input(log_csv_path)

print(log_file)
print(cols_dict)

def detect_lapsed_session(defined_session, session_dict, inactivity_period, current_time):
    for session_ip in session_dict.keys():
        lapsed_gap = int((current_time - session_dict[session_ip]['last_time']).total_seconds())
        if lapsed_gap > inactivity_period:
            duration = int((session_dict[session_ip]['last_time'] - session_dict[session_ip]['first_time']).total_seconds())+1
            defined_session[session_ip] = dict()
            defined_session[session_ip]['session_ip'] = session_ip
            defined_session[session_ip]['first_time'] = session_dict[session_ip]['first_time']
            defined_session[session_ip]['last_time'] = session_dict[session_ip]['last_time']
            defined_session[session_ip]['requests_num'] = session_dict[session_ip]['requests_num']
            defined_session[session_ip]['duration'] = duration
            del session_dict[session_ip]
    return defined_session, session_dict

def session_identifier(log_file,cols_dict,inactivity_period):
    session_dict = dict()
    defined_session = OrderedDict()
    k = 0

    for line in log_file:
        k += 1
        current_time = datetime.strptime('{0} {1}'.format(line[cols_dict['date']], line[cols_dict['time']]), '%Y-%m-%d %H:%M:%S')
        current_ip = line[cols_dict['ip']]

        if current_ip in session_dict.keys():
            session_dict[current_ip]['last_time'] = current_time
            session_dict[current_ip]['requests_num'] += 1

        else:
            session_dict[current_ip] = dict()
            session_dict[current_ip]['first_time'] = current_time
            session_dict[current_ip]['last_time'] = current_time
            session_dict[current_ip]['requests_num'] = 1

        if k < idx:
            defined_session, session_dict = detect_lapsed_session(defined_session, session_dict, inactivity_period, current_time)

        else:
            for rest_ip in session_dict.keys():
                defined_session[rest_ip] = dict()
                defined_session[rest_ip]['session_ip'] = rest_ip
                defined_session[rest_ip]['first_time'] = session_dict[rest_ip]['first_time']
                defined_session[rest_ip]['last_time'] = session_dict[rest_ip]['last_time']
                defined_session[rest_ip]['requests_num'] = session_dict[rest_ip]['requests_num']
                defined_session[rest_ip]['duration'] = int((session_dict[rest_ip]['last_time'] - session_dict[rest_ip]['first_time']).total_seconds())+1

    return defined_session

defined_session= session_identifier(log_file,cols_dict,inactivity_period)
print(defined_session)

def writer(defined_session,outfile):
        with open(outfile,"w") as f:
            for record in defined_session.values():
                f.write('{0},{1},{2},{3},{4}\n'.format(record['session_ip'], record['first_time'],
                            record['last_time'],record['duration'], record['requests_num']))

writer(defined_session,outfile)