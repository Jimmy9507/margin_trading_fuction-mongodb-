#!/usr/bin/env  python
import datetime
import calendar
import argparse
import os
import pandas as pd

__author__ = 'Johnson'


def _load_data(file_path):
    if not os.path.isfile(file_path):
        raise ("ERROR: Cannot load file:{0} since it does not exist.".format(file_path))
    df = pd.read_csv(file_path, index_col='date',
                     na_values=["None", "NaN", "Nan", "nan", "NAN", "NULL", "Null", "null", ""])
    return df.reindex([d.strftime('%Y-%m-%d') for d in list(pd.date_range(df.index.min(), df.index.max()))])


def _read_path_to_df(root_path):
    dirs = os.listdir(root_path)
    data = {}
    for dir in dirs:
        dirname, filename = os.path.split(dir)
        stock, postfix = os.path.splitext(filename)
        if postfix == ".csv":
            data[stock] = _load_data(root_path + "/" + dir)
    ret = pd.concat(data)
    return ret


def _get_days_sum(daylist, df):
    data = df.loc[(slice(None), daylist), :]
    last = data.xs(daylist[-1], level='date').loc[:, ['total_comments', 'followers']]
    total = data.loc[:,
            ['newly_comments', 'newly_followers', 'sell_actions', 'buy_actions', 'trading_actions']].sum(axis=0,
                                                                                                         level=0)
    return pd.concat([total, last], axis=1, join='inner')


def parse_args():
    description = "xueqiu sum data builder and updater"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--raw', '-r', required=True, type=str,
                        help="This para indicated rootpath of xueqiu's raw data, which include ./raw/[china|us]")
    parser.add_argument('--outpath', '-o', required=True, type=str, help="The dir which sum csv dump out")
    parser.add_argument('--start', '-s', type=str, default=None, help="indicated which day to stat build,YYYYmmdd")
    parser.add_argument('--end', '-e', required=True, type=str, help="indicated wich day build end, YYYYmmdd")
    args = parser.parse_args()
    args.end = datetime.datetime.strptime(args.end, "%Y%m%d")
    args.start = datetime.datetime.strptime(args.start, "%Y%m%d") if args.start else args.end
    if args.end < args.start:
        raise ValueError("End day must later than start day")
    return args


def lastMonthMaxDay(date):
    year = date.year
    month = date.month
    if month == 1:
        month = 12
        year -= 1
    else:
        month -= 1
    maxday = calendar.monthrange(year, month)[1]
    return maxday


def lastdays(date, count):
    m = list(range(count))
    m.reverse()
    for i in m:
        day = date - datetime.timedelta(days=i)
        yield day.strftime("%Y-%m-%d")


def build_month_sumcsv(df, end_day, outfile):
    stat = _get_days_sum(list(lastdays(end_day, lastMonthMaxDay(end_day))), df)
    # if stat.size > 0:
    #     stat.to_csv(outfile, float_format='%.f',index_label='order_book')
    # else:
    #     print("Build month {0} sum csv, but Dataframe is null".format(end_day))
    stat.to_csv(outfile, float_format='%.f', index_label='order_book')


def build_week_sumcsv(df, end_day, outfile):
    stat = _get_days_sum(list(lastdays(end_day, 7)), df)
    # if stat.size > 0:
    #     stat.to_csv(outfile, float_format='%.f',index_label='order_book')
    # else:
    #     print("Build week {0} sum csv, but Dataframe is null".format(end_day))
    stat.to_csv(outfile, float_format='%.f', index_label='order_book')


def build_day_sumcsv(df, end_day, outfile):
    stat = _get_days_sum(list(lastdays(end_day, 1)), df)
    # if stat.size > 0:
    #     stat.to_csv(outfile, float_format='%.f',index_label='order_book')
    # else:
    #     print ("Build day {0} sum csv, but Dataframe is null".format(end_day))
    stat.to_csv(outfile, float_format='%.f', index_label='order_book')


def build_sumcsv(start, end, rawpath, outpath):
    country = "china"
    df = _read_path_to_df(os.path.join(rawpath, country))
    while start <= end:
        try:
            csvpost = start.strftime("%Y%m%d")
            build_month_sumcsv(df, start, os.path.join(outpath, country) + "/" + csvpost + "_m" + ".csv")
            build_week_sumcsv(df, start, os.path.join(outpath, country) + "/" + csvpost + "_w" + ".csv")
            build_day_sumcsv(df, start, os.path.join(outpath, country) + "/" + csvpost + "_d" + ".csv")
            start = start + datetime.timedelta(days=1)
        except KeyError:
            start = start + datetime.timedelta(days=1)
            continue


def data_build():
    args = parse_args()
    build_sumcsv(args.start, args.end, args.raw, args.outpath)


def cmd_entry():
    args = parse_args()
    build_sumcsv(args.start, args.end, args.raw, args.outpath)
