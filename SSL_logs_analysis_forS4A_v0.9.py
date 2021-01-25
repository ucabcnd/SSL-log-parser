# LAST UPDATED: 09/09/2020
# V 0.7
# Script modified by Chenuka Ratwatte

import os
import operator
import time
import csv
import sys
import re
import datetime
import calendar
import math
import glob
from ua_parser import user_agent_parser
from tqdm import tqdm

from multiprocessing import Pool

maxInt = sys.maxsize

loglimit = 2 #Set this to the numer of logs to be compiled into output file at one time. This needs to be set to stop memory from overloading.

while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

s_path = os.getcwd()

logs = [os.path.join(root, name) for root, dirs, files in os.walk(s_path) for name in files if name.startswith('ssl_connected.s4a') and not name.endswith(('.gz', '.zip', '.7z', '.rar'))]

print("\n{0} log(s) found . . .".format(len(logs)))

#time_options = ['minute', 'hour', 'both']

## No. of seconds per time interval
time_intervals = {
    'minute': 60.0,
    'hour': 3600.0,
}

## dict for use in comparing and compiling response times and count
## format will be {[uristem, httpcode, time_interval],[count, responsetime]}
count_response = {}

servers = set()

fields_dict = {}
#Dictionary to map user journeys with the values and keys specidifed here

"""
Enter into the below dictionary the following format:

key:value,

where key = substring of URL you want to group by - must be uniquely identifiable for that URL eg: "/doc/svc/getDocument"
where Value = A list of platform and general transaction in the user journey eg: ['MOBILE','DOCUMENT FLOW']

"""
user_journey_dict = {
    '/doc/svc/getDocument': ['MOBILE','DOCUMENT FLOW']
}

months = {v: k for k, v in enumerate(calendar.month_abbr)}


def date_format(a, interval):
    date, hour, minute, second = a.split(':')
    day, month, year = date.split('/')
    dt = datetime.datetime(year=int(year), month=int(months[month]), day=int(day), hour=int(hour), minute=int(minute), second=int(second))
    if interval == 1:
        return dt.strftime("%d/%m/%Y %H:%M:00")
    elif interval == 60:
        return dt.strftime("%d/%m/%Y %H:00:00")
    else:
        return (dt - datetime.timedelta(minutes=dt.minute % interval, seconds=dt.second, microseconds=dt.microsecond)).strftime('%d/%m/%Y %H:%M:%S')


# Is used to read the number of lines in the file
def line_count_blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b: break
        yield b


def parse_single_line(line):
    fields_dict = {'s-ip': 0, 'a': 1, 'b': 2, 'date': 3, 'c': 4, 'cs-uri-stem': 5, 'httpcode': 6, 'bytes': 7, 'user-agent': 8, 'Server Name 1': 9, 'Server Name 2': 10, 'Session ID': 11, 'd': 12, 'time-taken': 13}
    interval = 60

    if not line[0].startswith('#'):
        if not any(x in line[fields_dict['cs-uri-stem']] for x in ['.png', '.css', '.js', '.ico', '.eot', '.ttf', '.gif', '.woff','.svg','image','logo']):
            if len(line) == len(fields_dict) or len(line) == len(fields_dict)+1:
                server_name = line[fields_dict['s-computername']].strip() if 's-computername' in fields_dict else line[fields_dict['s-ip']].strip()

                date_time_string = date_format(line[fields_dict['date']][1:], interval)
                uristem = line[fields_dict['cs-uri-stem']].strip().lower().split()[1].split('?')[0].split(';')[0]

                for key in user_journey_dict:
                    if key in uristem:
                        user_journey = user_journey_dict[key][1]
                        application = user_journey_dict[key][0]
                        break
                    else:
                        user_journey = 'Not Classified'
                        application = 'Not Classified'

                httpcode = line[fields_dict['httpcode']]
                sc_bytes = float(line[fields_dict['bytes']]) if line[fields_dict['bytes']].strip() != '-' else 0
                responsetime = int(line[fields_dict['time-taken']]) / 1000000.0 #converting microseconds to seconds

                try:
                    user_agent_device = user_agent_parser.ParseDevice(line[fields_dict['user-agent']])['brand'] + ' ' +user_agent_parser.ParseDevice(line[fields_dict['user-agent']])['family']
                except:
                    user_agent_device = 'Desktop PC'

                try:
                    user_agent_browser = user_agent_parser.ParseUserAgent(line[fields_dict['user-agent']])['family']
                except:
                    user_agent_browser = ''
                    
                if user_agent_browser == 'Mobile Safari UI/WKWebView' and user_agent_device == 'Apple iPad' and application == 'Not Classified':
                    application = 'MOBILE'
                elif application == 'Not Classified':
                    application = 'WEB'
                else:
                    pass

                return [date_time_string, uristem, httpcode, user_journey, user_agent_device, user_agent_browser, responsetime, sc_bytes, server_name, application]

def parser(item, interval):
    # To store the field names and their respective columns, so lookups can be done
    # against field names, rather than have to remember which colunm represents which field
    fields_dict = {'s-ip': 0, 'a': 1, 'b': 2, 'date': 3, 'c': 4, 'cs-uri-stem': 5, 'httpcode': 6, 'bytes': 7, 'user-agent': 8, 'Server Name 1': 9, 'Server Name 2': 10, 'Session ID': 11, 'd': 12, 'time-taken': 13}

    total_num_lines = 0

    with open(item, "r",encoding="utf-8",errors='ignore') as log:
        total_num_lines = sum(bl.count("\n") for bl in line_count_blocks(log))

    print("The total number of lines in this log file are: " +  str(total_num_lines))

    results = []
    pool = Pool(6)

    with open(item, "r",encoding="utf-8",errors='ignore') as log:
        reader = csv.reader(log, delimiter=' ')
        results = [i for i in pool.map(parse_single_line, reader, 10000) if i]

    for result in results:
        dict_filter = result[0], result[1], result[2], result[3], result[4], result[5], result[9]
        servers.add(result[8])

        if dict_filter in count_response:
            count_response[dict_filter] = ([count_response[dict_filter][0] + 1, count_response[dict_filter][1] + result[6], count_response[dict_filter][2] + result[7]])
        else:
            count_response[dict_filter] = [1, result[6], result[7]]

    print('Length of results in parser: {}'.format(len(results)))

    return results

def file_writer(interval, results, idx):
    sorted_dict = sorted(iter(count_response.items()), key=operator.itemgetter(0))
    if len(sorted_dict) > 100:
        print("Writing " + str(len(sorted_dict)) + " lines of data into output")
    else:
        print("For some reason we couldn't extract any data from this file...")
    print('Length or results in file_writer: {}'.format(len(results)))
    print(type(results))
    print(type(results[0]))

    processedfile = 'results_{0}_minutes_{1}.tsv'.format(interval,math.ceil(idx/loglimit))

    no_of_servers = len(servers)
    with open(processedfile, 'w', newline='') as newfile:
        writer = csv.writer(newfile, delimiter='\t')
        if idx<=loglimit:
            writer.writerow(['Date Time(hour)', 'Page', 'HTTPCode', 'Product', 'Device', 'Browser', 'Count', 'Average Response Time (s)', 'Avergage sc-bytes', 'Number of Servers', 'S4A Application'])
        for key, value in sorted_dict:
            avg_resp_time = value[1] / value[0]
            avg_sc_bytes = value[2] / value[0]
            writer.writerow([key[0], key[1], key[2], key[3], key[4], key[5], value[0], avg_resp_time, avg_sc_bytes, no_of_servers, key[6]])

        # writer.writerow(['Date Time(hour)', 'Page', 'HTTPCode', 'User Journey', 'Device', 'Browser', 'Response Time', 'sc-bytes'])
        # for row in results:
        #     writer.writerow(row)


def menu():
    user_options = input('\nWhat time summarisation (in minutes) do you want?\nSeparate with commas if multiple summarisations required\n(or press x and enter to exit) > ')
    if user_options == 'x':
        sys.exit(0)
    opts = [int(x.strip()) for x in user_options.split(',')]
    return opts
    
def combinefiles():
    outputfiles = [os.path.join(root, name) for root, dirs, files in os.walk(s_path) for name in files if name.startswith('results_') and not name.endswith(('.gz', '.zip', '.7z', '.rar'))]
    
    with open("combinedresult.tsv", "wb") as outfile:
        for f in outputfiles:    
            with open(f, "rb") as infile:
                outfile.write(infile.read())
            os.remove(f)


def main():

    times = [60]  #menu()

    start = time.time()

    for this_time in times:
        results = []
        for idx, item in enumerate(logs, start=1):
            print('\n{0} of {1}: Currently processing {2} by {3} minutes . . .'.format(idx, len(logs), item, this_time))
            interval = this_time
            results.extend(parser(item, this_time))
            if idx%loglimit == 0:
                file_writer(this_time, results, idx)
                results = []
                count_response.clear()
            elif idx == len(logs):
                file_writer(this_time, results, idx)
                results = []
            fields_dict.clear()

        print('Length of results in main: {}'.format(len(results)))
        count_response.clear()

    time_taken = time.time() - start

    combinefiles()
    
    print('\nCompleted')

    print('\nNumber of servers in logs parsed as part of this run: {0}'.format(len(servers)))

    print("\nThis run took {0} seconds\n".format(round(time_taken, 2)))


if __name__ == "__main__":
    main()
