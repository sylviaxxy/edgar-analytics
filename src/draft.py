from __future__ import absolute_import, division, print_function
from itertools import izip as zip
from datetime import datetime


infile1 = '/Users/xinyuxu/PycharmProjects/edgar-analytics/insight_testsuite/tests/test_1/input/log.csv'
infile2 = '/Users/xinyuxu/PycharmProjects/edgar-analytics/insight_testsuite/tests/test_1/input/inactivity_period.txt'
outfile = 'sessionization.txt'
with open(infile2 , 'rb') as f:
    for line in f:
        period = line.strip()
inactivity_period = int(period)
log_csv_path = infile1

def read_input(log_csv_path):

    """Read input file 'log.csv' and get necessary columns.
    """

    header = []
    log_file = []
    idx = 0
    col_names = ['ip','date','time','cik','accession','extention']
    col_index = []
    with open(log_csv_path, 'rb') as logs:
        for line in logs:
            if idx == 0:
                line = line.strip().split(",")
                header = line
                idx += 1
                for col_name in col_names:
                    col_index.append(header.index(col_name))
                cols_dict = {k: v for k, v in zip(header, col_index)}

            else:
                line = line.strip().split(",")
                line = [line[i] for i in col_index]
                log_file.append(line)
                idx += 1

    return log_file, cols_dict

log_file, cols_dict = read_input(log_csv_path)

print(log_file)
print(cols_dict)


def session_identifier(log_file,cols_dict):
    ip_dict = {'ip_address':0,'':0,'':0}

    for line in log_file:

        new_time = datetime.datetime.strptime('{0} {1}'.format(line[cols_dict['date']], line[cols_dict[time]]), '%Y-%m-%d %H:%M:%S')
        if 'current_time' not in vars():
            time_delta = 0
        else:
            time_delta = int((new_time - current_time).total_seconds())
            current_time = new_time

        current_ip = line[cols_dict[ip]]

        if current_ip in ip_dict{}:

            ip_idct[current_ip][''] += 1
            ip_idct[current_ip][] =