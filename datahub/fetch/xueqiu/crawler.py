import urllib.request
import re
import argparse
import sys
import os
import csv

theurl = 'http://183.131.12.35:8989/xueqiu/'
username = 'admin'
password = 'admin321'
xueqiu_folder = 'xueqiu'
root_folder = ''

column_names = ['date', 'total_comments', 'newly_comments', 'followers', 'newly_followers', 'sell_actions',
                'buy_actions', 'trading_actions']


def parse_args():
    description = "This script will download the xueqiu crawl data and put into a /config-root-folder/xueqiu/[china|hk] folder"
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--folder', '-f')

    args = parser.parse_args()

    if args.folder is None:
        # print("ERROR: Cannot run the xueqiu crawler script due to root folder is not set, -f is required.")
        sys.exit(1)

    global root_folder
    root_folder = args.folder + "/"
    # print(root_folder)


def authorize():
    # this creates a password manager
    passman = urllib.request.HTTPPasswordMgrWithDefaultRealm()

    # because we have put None at the start it will always
    # use this username/password combination for  urls
    # for which `theurl` is a super-url
    passman.add_password(None, theurl, username, password)

    # create the AuthHandler
    authhandler = urllib.request.HTTPBasicAuthHandler(passman)

    opener = urllib.request.build_opener(authhandler)

    # All calls to urllib2.urlopen will now use our handler
    # Make sure not to include the protocol in with the URL, or
    # HTTPPasswordMgrWithDefaultRealm will be very confused.
    # You must (of course) use it when fetching the page though.
    urllib.request.install_opener(opener)

    # authentication is now handled automatically for us`


def build_path(country):
    return root_folder + "/" + country


def create_folder(country):
    country_path = build_path(country)
    if not os.path.exists(country_path):
        # print("country_path: " + country_path)
        os.makedirs(country_path)


# build the local store file name.
def build_csv_file_path(remote_csv):
    split_file_names = remote_csv.split('.csv')
    stock_code = split_file_names[0]
    file_name = ''
    country = 'china'
    if stock_code[0:2] == 'SH':
        file_name = stock_code[2:] + ".XSHG.csv"
    elif stock_code[0:2] == 'SZ':
        file_name = stock_code[2:] + ".XSHE.csv"
    else:
        file_name = stock_code[0:] + ".HKSE.csv"
        country = 'hk'

    # Considering the folder
    file_path = build_path(country) + "/" + file_name
    return file_path


def build_download_csv_url(stock_code_csv):
    ##print("stock_code_csv: " + stock_code_csv)
    return theurl + stock_code_csv


def download_csv(stock_code_csv):
    csv_url = build_download_csv_url(stock_code_csv)
    ##print('csv_url: ' + csv_url)
    attempts = 0
    while attempts < 5:
        try:
            pagehandle = urllib.request.urlopen(csv_url)
            break
        except Exception as err:
            attempts += 1
            print("ERROR: Cannot get csv file at url: " + csv_url)
            print (err.format_exec())
            # return
    if attempts == 5:
        sys.exit(1)
    csv_html = pagehandle.read()

    # open the file
    file_path = build_csv_file_path(stock_code_csv)
    ##print("saving to file: " + file_path)

    # strip the first two, since it's b', and also strip the last one since it's '
    data = (str(csv_html))[2:-1]
    # #print(data)

    localFile = open(file_path, 'w')
    csvwriter = csv.writer(localFile)

    rows = data.split('\\n')

    # first row should be the column names.
    csvwriter.writerow(column_names)

    for row in rows:
        row_array = row.split(',')
        if len(row_array) > 1:
            # #print("row array - " + str(row_array))
            csvwriter.writerow(row_array[1:])

    localFile.close()


def download_csvs(exchange_ins_list):
    for stock in exchange_ins_list:
        # build the csv file name
        download_csv(stock)


def crawl_all_instruments():
    pagehandle = urllib.request.urlopen(theurl)
    html = pagehandle.read()
    # SZ, SH and HK
    stock_code_dict = {'SH': [], 'HK': []}

    all_sz_ins_list = re.findall(r'SZ\d{6}.csv', str(html))
    stock_code_dict['SZ'] = set(all_sz_ins_list)

    all_sh_ins_list = re.findall(r'SH\d{6}.csv', str(html))
    stock_code_dict['SH'] = set(all_sh_ins_list)

    all_hk_ins_list = re.findall(r'"\d{5}.csv', str(html))
    stock_code_dict['HK'] = set()
    for hk_stock_code in all_hk_ins_list:
        # remove the first " by starting from 1
        stock_code_dict['HK'].add(hk_stock_code[1:])

    # build china and hk folders:
    create_folder('china')
    create_folder('hk')

    # loop through SZ
    # for file_name in stock_code_dict['SZ']:
    # build_csv_file_name(file_name)
    download_csvs(stock_code_dict['SZ'])
    # print("Finished download xueqiu csvs for SZ market.")

    # for SH
    download_csvs(stock_code_dict['SH'])
    # print("Finished download xueqiu csvs for SH market.")

    # for HK
    download_csvs(stock_code_dict['HK'])
    # print("Finished download xueqiu csvs for HK market.")


def data_crawl():
    parse_args()
    authorize()
    crawl_all_instruments()
    download_csv('SZ000001.csv')


def cmd_entry():
    parse_args()
    authorize()
    crawl_all_instruments()
