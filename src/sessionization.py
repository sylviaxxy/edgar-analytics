#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Transform web log of edgar into session records

@author: Xinyu (Sylvia) Xu

"""

# import necessary packages

from __future__ import absolute_import, division, print_function
import os
import sys
from datetime import datetime


class Sessionization():

    """
          Read a file with weblogs
          Yields sessionization results
    """

    def __init__(self):
        infile1 = '/Users/xinyuxu/PycharmProjects/edgar-analytics/insight_testsuite/tests/test_1/input/log.csv'
        infile2 = '/Users/xinyuxu/PycharmProjects/edgar-analytics/insight_testsuite/tests/test_1/input/inactivity_period.txt'
        outfile = 'sessionization.txt'
        with open(infile2 , 'rb') as f:
            for line in f:
                period = line.strip()
        self.inactivity_period = period
        self.log_csv_path = infile1

    def read_input(self):

        """Read input file (named by 'self.manifest') and get necessary columns.
        """
        log_file=[]
        with open(self.log_csv_path, 'rb') as logs:
            for line in logs:

                line = line.strip()

                log_file.append(line)
        return log_file

    #def session_identifier(self):



    def write_records(self):

        """Write changed .pth file back to disk"""

    def run(self):
        self.read_input()

##### Just to test #######
worker = Sessionization()
worker.run()
