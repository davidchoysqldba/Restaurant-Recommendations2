import glob
import os
from datetime import date as d, timedelta
import logging
import pandas as pd

import sys
import os
sys.path.insert(0, '../')

log = logging.getLogger(__name__)

def getdownloadedNthFiles(path_name, pattern, n_th=None, last_n_th=None):

    file_search_query = None
    try:
        file_search_query = os.path.join(path_name, pattern)
    except Exception as e:
        log.error('Exception at module/location ' + __name__ + ' (' + str(type(e)) + ', ' + str(e) + ')  ')

    if n_th != None:
        return sorted([i for i in glob.glob(file_search_query)])[n_th:]
    elif last_n_th != None:
        return sorted([i for i in glob.glob(file_search_query)], reverse=True)[:last_n_th]
    else:
        return sorted([i for i in glob.glob(file_search_query)])


def getAddDays(the_date, number_go_to, date_type):
    import dateutil.relativedelta as reldate
    # print(thedate)
    # print(number_goto)
    # print(datetype)
    result_date = None
    days_to_go = None

    # if number_go_to < 0:
    #     days_to_go = -1 * number_go_to

    try:
        if date_type == 'days':
            days_to_go = timedelta(days=number_go_to)
        elif date_type == 'month':
            days_to_go = reldate.relativedelta(months=number_go_to)
        else:
            days_to_go = reldate.relativedelta(months=number_go_to)
    except Exception as e:
        log.error('Exception at module/location ' + __name__ + ' (' + str(type(e)) + ', ' + str(e) + ') ')

    #print('days_to_go', days_to_go)

    result_date = the_date + days_to_go
    #print(result_date)
    return result_date


def getDateRange(prefix, date_start, date_end, suffix):
    date_list = []
    date_loop = date_start
    date_string = ''
    while date_loop <= date_end:
        if prefix != None:
            date_string = prefix
        date_string += str(date_loop)
        if suffix != None:
            date_string += suffix

        date_list.append(date_string)
        date_loop = date_loop + timedelta(days=1)  # replace the interval at will
    return date_list

def getDateRange2(prefix, date_start, date_end, suffix):
    date_list = []
    date_loop = date_start
    date_string = ''

    date_list = sorted(pd.date_range(date_start, date_end).tolist())

    # while date_loop <= date_end:
    #     if prefix != None:
    #         date_string = prefix
    #     date_string += str(date_loop)
    #     if suffix != None:
    #         date_string += suffix
    #
    #     date_list.append(date_string)
    #     date_loop = date_loop + timedelta(days=1)  # replace the interval at will

    return date_list


def main():


    # try:
    #     1 / 0
    # except Exception as e:
    #     log.error('Error at ' + __name__ + ' (' + str(type(e)) + ', ' + str(e) + ') ')
    # finally:
    #     pass

    # path_name = '/opt/app/python/app/restaurant_recommendation/data232s'
    # pattern_name = 'restaurant_data*json'
    # downloaded_file_list = getdownloadedNthFiles(path_name, pattern_name)
    # log.info(downloaded_file_list)
    #
    # test_date = getAddDays(d.today(), 13, 'month')
    # log.info(test_date)


    date_end = getAddDays(d.today(), -3, 'days')
    # log.info(date_end)

    date_start = getAddDays(date_end, -3, 'month')
    # log.info(date_start)


    dlist = getDateRange2('', date_start, date_end, '')
    # print(list)
    print(dlist[:1])
    # print(dlist[len(dlist)-1::])

    # ---------------------------------------------------------------------------------------
    # function begins

    # path_name = '/opt/app/python/app/restaurant_recommendation/data'
    # pattern_name = 'restaurant_data*json'
    # #file_name = getdownloadedNthFiles(path_name, pattern_name, 1260)
    # #file_name = getdownloadedNthFiles(path_name, pattern_name, -2)
    # #print(file_name)
    # date_end = getAddDays(d.today(), -3, 'days')
    # #print(date_end)
    #
    # date_start = getAddDays(date_end, -3, 'month')
    # #print(date_start)
    #
    # downloaded_file_list = getdownloadedNthFiles(path_name, pattern_name)
    # # print(downloaded_file_list)
    # # print('')
    #
    # downloaded_files = [i.replace(path_name + '/', '') for i in downloaded_file_list]
    # # print('downloaded files: ', downloaded_files)
    # # print('')
    #
    # files_of_date_range = getDateRange(pattern_name.split('*')[0] + '_', date_start, date_end, '.json')
    # files_of_date_range = [i.replace('-', '_') for i in files_of_date_range]
    # # print('files_of_date_range:', files_of_date_range)
    # # print('')
    #
    # # print(sorted(getDateRange(date_start, date_end), reverse=True))
    #
    # downloading_list = set(files_of_date_range) - set(downloaded_files)
    # print('downloading_list: ', sorted(downloading_list))

    # function ends
    # ---------------------------------------------------------------------------------------


if __name__ == '__main__':
    main()
