#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/9 11:57
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : adwords_capture.py
# @Software: PyCharm
# @Desc    :

from datetime import datetime
from googleads import adwords, oauth2, errors
from Queue import Empty
from retrying import retry

from adwords_mcces import adwords_tasks, report_defines, adwords_mccess
from logger import logger

import pandas as pd
import numpy as np
import re
import copy
import multiprocessing
import os
import time


MAX_TIMES = 3
ADW_VERSION = 'v201806'
# Timeout between retries in seconds.
BACKOFF_FACTOR = 5
# Maximum number of processes to spawn.
MAX_PROCESSES = multiprocessing.cpu_count()
# Maximum number of retries for 500 errors.
MAX_RETRIES = 5
# Maximum number of items to be sent in a single API response.
PAGE_SIZE = 300


date_now = datetime.now().strftime("%Y%m%d%H%M%S")
# date_now = '20180815205050'
current_dir = os.path.abspath(os.path.dirname(__file__))
work_dir = os.path.join(current_dir, 'adwords'+date_now)
report_dir = os.path.join(work_dir, 'report')
conf_dir = os.path.join(current_dir, 'conf')


#Something's not right!
class Retry500Exception(Exception):
    def __init__(self, err='adwords 500 error'):
        super(Retry500Exception, self).__init__(err)

def retry_if_500_error(exception):
    return isinstance(exception, Retry500Exception)

class AppWorker(multiprocessing.Process):
    """A worker Process used to download reports for a set of customer IDs."""

    def __init__(self, mcc, adwords_task, input_queue, success_queue, failure_queue):
        """Initializes a ReportWorker.
    Args:
      mcc: mcc id
      adwords_task :
      input_queue: A Queue instance containing all of the customer IDs that
        the report_definition will be run against.
      success_queue: A Queue instance that the details of successful report
        downloads will be saved to.
      failure_queue: A Queue instance that the details of failed report
        downloads will be saved to.
        """
        super(AppWorker, self).__init__()
        self.mcc = mcc
        self.adwords_task = adwords_task
        self.input_queue = input_queue
        self.success_queue = success_queue
        self.failure_queue = failure_queue

    def run(self):
        adwords_client = get_adwords_client(self.mcc)
        r = {True: self.success_queue,
             False: self.failure_queue}
        while True:
            try:
                logger.debug('{} app input_queue: {}'.format(self.ident, self.input_queue.qsize()))
                info = self.input_queue.get(timeout=0.01)
                if isinstance(info, list):
                    customer_id = info[0]
                    campaign_id = info[1]
                else:
                    customer_id = info
                    campaign_id = []
            except Empty:
                break
            result = get_adwords_apps(adwords_client, customer_id, campaign_id)
            r.get(result[0]).put(result[1])



class ReportWorker(multiprocessing.Process):
    """A worker Process used to download reports for a set of customer IDs."""

    def __init__(self, mcc, adwords_task, report_download_directory, report_definition, input_queue, success_queue, empty_queue, failure_queue):
        """Initializes a ReportWorker.
    Args:
      report_download_directory: A string indicating the directory where you
        would like to download the reports.
      report_definition: A dict containing the report definition that you would
        like to run against all customer IDs in the input_queue.
      input_queue: A Queue instance containing all of the customer IDs that
        the report_definition will be run against.
      success_queue: A Queue instance that the details of successful report
        downloads will be saved to.
      failure_queue: A Queue instance that the details of failed report
        downloads will be saved to.
        """
        super(ReportWorker, self).__init__()
        self.mcc = mcc
        self.adwords_task = adwords_task
        self.report_download_directory = report_download_directory
        self.report_definition = report_definition
        self.input_queue = input_queue
        self.success_queue = success_queue
        self.empty_queue = empty_queue
        self.failure_queue = failure_queue

    def run(self):
        try:
            adwords_client = get_adwords_client(self.mcc)
            r = {0: self.success_queue,
                 1: self.empty_queue,
                 2: self.failure_queue}
            while True:
                try:
                    logger.info('{} input_queue: {}'.format(self.ident, self.input_queue.qsize()))
                    customer_id = self.input_queue.get(timeout=0.01)
                except Empty:
                    logger.debug('{} empty'.format(self.ident))
                    break
                task_type = self.adwords_task.get('task_type')
                report_type = report_defines.get(task_type)
                file_csv_name = 'adwords_{}_{}_report_{}.csv'.format(task_type, customer_id, report_type.dateRangeType)
                report_download_file = os.path.join(self.report_download_directory, file_csv_name)
                result = _DownloadReport(self.ident, adwords_client, report_download_file, customer_id, self.report_definition)
                r.get(result[0]).put(result[1])
        except Exception, ex:
            logger.error('{} error {}'.format(self.ident, ex))

def get_csv_files_and_ids(path, s0, s1):
    result = {}
    for file in os.walk(path):
        for each_list in file[2]:
            if re.search(s0, each_list):
            # os.walk()函数返回三个参数：路径，子文件夹，路径下的文件，利用字符串拼接file[0]和file[2]得到文件的路径
                id = re.search(s1, each_list).group(0)
                csv_path = os.path.join(path, each_list.strip())
                result[id] = csv_path
    return result

def generate_location_csv(csv_file, conf_dir):
    try:
        pandas_data = pd.read_csv(csv_file)
        #去掉Country Code为空的行
        null = pandas_data[pandas_data['Country Code'].isnull()].index.tolist()
        pandas_data = pandas_data.drop(null)
        #去掉列中重复的行
        pandas_data.drop_duplicates('Criteria ID', 'first', True)
        pandas_data.drop_duplicates('Parent ID', 'first', True)
        if not os.path.exists(conf_dir):
            os.mkdir(conf_dir)
        location_csv_file = os.path.join(conf_dir, 'adwords_location.csv')
        pandas_data['Parent ID'] = pandas_data['Parent ID'].fillna(0)
        x = pandas_data['Parent ID'].astype(int)
        pandas_data = pandas_data.drop('Parent ID', axis=1)
        pandas_data = pandas_data.join(x)
        pandas_data = pandas_data.sort_values(by="Criteria ID")
        pandas_data.to_csv(location_csv_file, index=False, columns=['Criteria ID', 'Parent ID', 'Country Code'])
    except Exception:
        raise

def merge_reports(work_dir, result_dir, task_type):
    try:
        logger.debug('merge_reports begin')
        report_type = report_defines.get(task_type)
        s0 = '^adwords_{}_\d+_report_{}.csv$'.format(task_type, report_type.dateRangeType)
        s1 = '\d+'
        results = get_csv_files_and_ids(work_dir, s0, s1)
        if not results:
            logger.info('not download any report file')
            return None
        if not os.path.exists(result_dir):
            os.mkdir(result_dir)
        integration_file = 'adwords_{}_report_{}.csv'.format(task_type, 'tmp')
        integration_path = os.path.join(report_dir, integration_file)
        columns = copy.copy(report_type.fields)
        columns.append('AccountId')
        for account_id, report_path in results.iteritems():
            pandas_data = pd.read_csv(report_path, header=None, names=report_type.fields)
            pandas_data['AccountId'] = account_id
            if os.path.exists(integration_path):
                pandas_data.to_csv(integration_path,  index=False, mode='a', header=False, columns=columns)
                logger.debug('merge_reports add {}'.format(account_id))
            else:
                pandas_data.to_csv(integration_path,  index=False, mode='w', columns=columns)
                logger.debug('merge_reports write {}'.format(account_id))
            #删除下载的report
            # os.remove(report_path)
        logger.debug('merge_reports end')
        return integration_path
    except Exception:
        raise

#会有垃圾数据1454474116,GOG_IOS_World_UAC_IAP_20180626_LY, --,2018-07-17,664798,1397,817.00,1430020359  Id为 --
def integration_geo_info(location_report, location_conf):
    try:
        pandas_report = pd.read_csv(location_report)
        pandas_conf = pd.read_csv(location_conf)
        pandas_report['CountryCode'] = np.nan
        ids = pandas_report['Id'].drop_duplicates().tolist()
        for i in ids:
            try:
                i_map = int(i)
            except ValueError, ex:
                logger.error('find Id type error:{}'.format(ex))
                continue
            map_Criteria_ID = pandas_conf.loc[pandas_conf['Criteria ID'] == i_map]['Country Code'].tolist()
            if map_Criteria_ID and map_Criteria_ID[0]:
                index_i = pandas_report.loc[pandas_report['Id'] == i].index
                pandas_report.loc[index_i, 'CountryCode'] = map_Criteria_ID[0]
                continue
            map_Parent_ID = pandas_conf.loc[pandas_conf['Parent ID'] == i_map]['Country Code'].tolist()
            if map_Parent_ID and map_Parent_ID[0]:
                index_i = pandas_report.loc[pandas_report['Id'] == i].index
                pandas_report.loc[index_i, 'CountryCode'] = map_Parent_ID[0]
        # 修改列名
        # a.rename(columns={'A': 'a', 'B': 'b', 'C': 'c'}, inplace=True)
        result_report = location_report.replace('_report_tmp.csv', '_report_{}.csv'.format(date_now))
        pandas_report = pandas_report.sort_values(by=["CampaignId", 'Date'])
        pandas_report.to_csv(result_report, index=False,  columns=['CampaignId', 'Date', 'Impressions', 'Clicks', 'Conversions', 'Cost', 'Id', 'CountryCode', 'AccountId'])
        return True
    except Exception:
        raise

def check_setting(infos):
    if infos['Setting.Type'] == 'UniversalAppCampaignSetting':
        return True

@retry(retry_on_exception=retry_if_500_error, wait_fixed=5000)
def get_adwords_apps(client, customerId, campaignId = []):
    try:
        logger.debug('customerId:{} begin'.format(customerId))
        campaign_map_app = {c_id:'' for c_id in campaignId}
        client.SetClientCustomerId(customerId)
        campaign_service = client.GetService('CampaignService', version=ADW_VERSION)
        # Construct selector and get all campaigns.
        offset = 0
        if campaignId:
            selector = {
                'fields': ['Id', 'Settings'],
                'predicates': [
                    {
                        'field': 'Id',
                        'operator': 'IN',
                        'values': campaignId
                    }
                ],
                'paging': {
                    'startIndex': str(offset),
                    'numberResults': str(PAGE_SIZE)
                }
            }
        else:
            selector = {
                'fields': ['Id', 'Settings'],
                'paging': {
                    'startIndex': str(offset),
                    'numberResults': str(PAGE_SIZE)
                }
            }
        more_pages = True
        while more_pages:
            page = campaign_service.get(selector)
            # Display results.
            if 'entries' in page:
                for campaign in page['entries']:
                    campaign_id = campaign.id
                    settings = filter(check_setting, campaign.settings)
                    if settings:
                        campaign_map_app[campaign_id] = settings[0].appId
            else:
                logger.debug('customerId: {} No campaigns were found.'.format(customerId))
            offset += PAGE_SIZE
            selector['paging']['startIndex'] = str(offset)
            more_pages = offset < int(page['totalNumEntries'])
        return True, {'customerId': customerId, 'campaign_map_app': campaign_map_app}
    except errors.AdWordsReportError, e:
        if e.code == 500:
            logger.error('customerId:{},campaignId:{} raise 500, will retry'.format(customerId, campaignId))
            raise retry_if_500_error
        else:
            return False, {'customerId': customerId, 'campaignId': campaignId, 'e': e}
    except Exception, e:
        logger.error('customerId: {} error {}'.format(customerId, e))
        return False, {'customerId': customerId, 'campaignId':campaignId, 'e': e}


def get_adwords_client(mcc):
    oauth2_client = oauth2.GoogleRefreshTokenClient(client_id=adwords_mccess.get(mcc).client_id,
                                                    client_secret=adwords_mccess.get(mcc).client_secret,
                                                    refresh_token=adwords_mccess.get(mcc).refresh_token)
    adwords_client = adwords.AdWordsClient(adwords_mccess.get(mcc).developer_token, oauth2_client,
                                           adwords_mccess.get(mcc).user_agent,
                                           client_customer_id=adwords_mccess.get(mcc).client_customer_id)
    return adwords_client

#only get customerid into queue
def GetCustomerIDs(client, queue, down_success=[]):
    """Retrieves all CustomerIds in the account hierarchy.
    Note that your configuration file must specify a client_customer_id belonging
    to an AdWords manager account.
    Args:
    client: an AdWordsClient instance.
    down_success: have been download will skip it
    Raises:
    Exception: if no CustomerIds could be found.
    Returns:
    A Queue instance containing all CustomerIds in the account hierarchy.
    """
    # For this example, we will use ManagedCustomerService to get all IDs in
    # hierarchy that do not belong to MCC accounts.
    managed_customer_service = client.GetService('ManagedCustomerService', version=ADW_VERSION)
    offset = 0
    # Get the account hierarchy for this account.
    selector = {
        'fields': ['CustomerId'],
        'predicates': [{
          'field': 'CanManageClients',
          'operator': 'EQUALS',
          'values': [False]
        }],
        'paging': {
          'startIndex': str(offset),
          'numberResults': str(PAGE_SIZE)
        }
    }
    # Using Queue to balance load between processes.
    more_pages = True

    while more_pages:
        page = managed_customer_service.get(selector)

        if page and 'entries' in page and page['entries']:
            for entry in page['entries']:
                if entry['customerId'] not in down_success:
                    queue.put(entry['customerId'])
                else:
                    logger.debug('customerId: {} have been done'.format(entry['customerId']))
        else:
            raise Exception('Can\'t retrieve any customer ID.')
        offset += PAGE_SIZE
        selector['paging']['startIndex'] = str(offset)
        more_pages = offset < int(page['totalNumEntries'])

def _DownloadReport(process_id, adwords_client, report_download_file, customer_id, report_definition):
    """Helper function used by ReportWorker to download customer report.
    Note that multiprocessing differs between Windows / Unix environments. A
    Process or its subclasses in Windows must be serializable with pickle, but
    that is not possible for AdWordsClient or ReportDownloader. This top-level
    function is used as a work-around for Windows support.
    Args:
    process_id: The PID of the process downloading the report.
    report_download_file: A string indicating the directory where you
        would like to download the reports.
    customer_id: A str AdWords customer ID for which the report is being
        downloaded.
    report_definition: A dict containing the report definition to be used.
    Returns:
    A tuple indicating a boolean success/failure status, and dict request
    context.
    """
    report_downloader = adwords_client.GetReportDownloader(version=ADW_VERSION)
    retry_count = 0

    while True:
        logger.debug('[%d/%d] Loading report for customer ID "%s" into "%s"...'
           % (process_id, retry_count, customer_id, report_download_file))
        try:
            with open(report_download_file, 'wb') as handler:
                report_downloader.DownloadReport(report_definition, output=handler, client_customer_id=customer_id,\
                                                 skip_report_header=True, skip_column_header=True, skip_report_summary=True, include_zero_impressions=False)
            if not os.path.getsize(report_download_file):
                os.remove(report_download_file)
                return 1, {'customerId': customer_id}
            else:
                return 0, {'customerId': customer_id}
        except errors.AdWordsReportError as e:
            if e.code == 500 and retry_count < MAX_RETRIES:
                retry_count+=1
                time.sleep(retry_count * BACKOFF_FACTOR)
            else:
                logger.debug('Report failed for customer ID "%s" with code "%d" after "%d" '
               'retries.' % (customer_id, e.code, retry_count))
                return 2, {'customerId': customer_id, 'code': e.code,'message': e.message}
        except Exception, e:
            logger.debug('Report failed for customer ID "%s" with code "%d"' % (customer_id, e.code))
            return 2, {'customerId': customer_id, 'code': e.code, 'message': e.message}


def parallel_report_downloads(report_download_directory, adwords_task):
    mcces = adwords_task.get('mcc')
    report_type = report_defines.get(adwords_task.get('task_type'))
    down_success = []
    # Create report definition.
    report_definition = {
        'reportName': '{} {}'.format(report_type.dateRangeType, report_type.reportType),
        'dateRangeType': report_type.dateRangeType,
        'reportType': report_type.reportType,
        'downloadFormat': 'CSV',
        'selector': {
            'fields': report_type.fields
            }
        }
    for mcc in mcces:
        adwords_client = get_adwords_client(mcc)
        input_queue = multiprocessing.Queue()
        GetCustomerIDs(adwords_client, input_queue, down_success)
        logger.info('parallel_report_downloads mcc: {} input_queue: {}'.format(mcc, input_queue.qsize()))
        manager = multiprocessing.Manager()
        reports_succeeded = manager.Queue()
        #bug  使用reports_succeeded会出现当queue里面放入东西过多（几百个）时 子线程不能正常退出
        # reports_succeeded = multiprocessing.Queue()
        reports_failed = manager.Queue()
        reports_empty = manager.Queue()
        queue_size = input_queue.qsize()
        num_processes = min(queue_size, MAX_PROCESSES)
        logger.debug('Retrieving %d reports with %d processes:' % (queue_size, num_processes))
      # Start all the processes.
        processes = [ReportWorker(mcc, adwords_task, report_download_directory, report_definition, input_queue, reports_succeeded, \
                                  reports_empty, reports_failed) for _ in range(num_processes)]
        for process in processes:
            process.start()
        for process in processes:
            process.join()

        logger.debug('mcc:{} Finished downloading reports with the following results:'.format(mcc))
        while True:
            try:
                success = reports_succeeded.get(timeout=0.01)
                down_success.append(success['customerId'])
            except Empty:
                break
            logger.debug('Report for CustomerId "%d" succeeded.' % success['customerId'])
        logger.debug('download success {} report'.format(len(down_success)))
        while True:
            try:
                empty = reports_empty.get(timeout=0.01)
            except Empty:
                break
            logger.debug('Report for CustomerId "%d" empty.' % empty['customerId'])

        while True:
            try:
                failure = reports_failed.get(timeout=0.01)
            except Empty:
                break
            logger.error('Report for CustomerId "%d" failed with error code "%s" and message: %s.' % (failure['customerId'], failure['code'], failure['message']))


def generate_merged_report(adwords_task):
    if not os.path.exists(work_dir):
        os.mkdir(work_dir)
    logger.info('{} report download begin'.format(adwords_task.get('task_type')))
    parallel_report_downloads(work_dir, adwords_task)
    logger.info('all report download finish')
    return merge_reports(work_dir, report_dir, adwords_task.get('task_type'))

def generate_location_report(adwords_task):
    try:
        location_csv_file = os.path.join(conf_dir, 'location.csv')
        if not os.path.exists(location_csv_file):
            logger.error('location_csv_file not exist')
            return False
        report_file = generate_merged_report(adwords_task)
        if not report_file or not os.path.exists(report_file):
            logger.error('report_file not generate')
            return False
        integration_geo_info(report_file, location_csv_file)
        # os.remove(report_file)
        return True
    except Exception, ex:
        logger.error('generate_location_report error:{}'.format(ex))
        return False


def generate_campaign_report(report_file):
    try:
        logger.info('generate_campaign_report begin')
        pandas_report = pd.read_csv(report_file)
        result_report = report_file.replace('_report_tmp.csv', '_report_{}.csv'.format(date_now))
        columns = pandas_report.columns.values.tolist()
        columns.remove('CampaignName')
        pandas_report = pandas_report[columns].sort_values(by=["CampaignId", 'Date'])
        pandas_report.to_csv(result_report, index=False)
        logger.info('generate_campaign_report success end')
        return True
    except Exception:
        logger.error('generate_campaign_report faile end')
        raise


def generate_campaign_app_conf(app_csv_file, adwords_task):
    mcces = adwords_task.get('mcc')
    get_success = []
    campaign_map_app_l = []
    campaign_map_app = {}
    for mcc in mcces:
        adwords_client = get_adwords_client(mcc)
        input_queue = multiprocessing.Queue()
        GetCustomerIDs(adwords_client, input_queue, get_success)
        if not input_queue.empty():
            manager = multiprocessing.Manager()
            reports_succeeded = manager.Queue()
            reports_failed = manager.Queue()
            queue_size = input_queue.qsize()
            num_processes = min(queue_size, MAX_PROCESSES)
            logger.debug('Retrieving %d reports with %d processes:' % (queue_size, num_processes))
            # Start all the processes.
            processes = [AppWorker(mcc, adwords_task, input_queue, reports_succeeded, reports_failed) for _ in range(num_processes)]
            for process in processes:
                process.start()
            for process in processes:
                process.join()
            logger.debug('mcc:{} Finished get app with the following results:'.format(mcc))
            while True:
                try:
                    success = reports_succeeded.get(timeout=0.01)
                    get_success.append(success['customerId'])
                    campaign_map_app.update(success['campaign_map_app'])
                except Empty:
                    break
                logger.debug('App for CustomerId "%d" succeeded.' % success['customerId'])

            while True:
                try:
                    failure = reports_failed.get(timeout=0.01)
                except Empty:
                    break
                logger.error('App for CustomerId "{}" failed with error message: {}.'.format(failure['customerId'], failure['e']))

    for key, value in campaign_map_app.iteritems():
        t = {'Times': 0}
        t['CampaignId'] = key
        t['AppId'] = value
        campaign_map_app_l.append(t)
    pandas_conf = pd.DataFrame(campaign_map_app_l).sort_values(by="CampaignId")
    pandas_conf.to_csv(app_csv_file, index=False, columns=['CampaignId', 'AppId', 'Times'])

def update_app_csv(app_conf, campaign_map_app):
    try:
        logger.info('begin update_app_csv file')
        pandas_conf = pd.read_csv(app_conf)
        need_add = []
        flag_wr = False
        for campaign, app in campaign_map_app.iteritems():
            map_app = pandas_conf.loc[pandas_conf['CampaignId'] == campaign][['AppId', 'Times']]
            if map_app.empty:
                add = {'Times': 0}
                add['CampaignId'] = campaign
                add['AppId'] = app
                need_add.append(add)
            else:
                flag_wr = True
                if app:
                    index = pandas_conf.loc[pandas_conf['CampaignId'] == campaign].index
                    pandas_conf.loc[index, ['AppId', 'Times']] = [app, 0]
                else:
                    times = map_app['Times'].values[0]
                    index = pandas_conf.loc[pandas_conf['CampaignId'] == campaign].index
                    pandas_conf.loc[index, ['AppId', 'Times']] = [app, times+1]
        if flag_wr:
            pandas_conf.to_csv(app_conf, index=False, mode='w', columns=['CampaignId', 'AppId', 'Times'])
        if need_add:
            pandas_conf = pd.DataFrame(need_add)
            pandas_conf.to_csv(app_conf, index=False, mode='a', header=False, columns=['CampaignId', 'AppId', 'Times'])
        logger.info('update_app_csv end')
        return True
    except Exception,ex:
        logger.error('campaign_map_app:{}'.format(campaign_map_app))
        return False

def integration_app_info(campaign_report, app_conf, adwords_task):
    try:
        pandas_report = pd.read_csv(campaign_report)
        pandas_conf = pd.read_csv(app_conf)
        pandas_report = pandas_report[['AccountId', 'CampaignId', 'CampaignName']].drop_duplicates()
        pandas_report['AppId'] = np.nan
        infos = pandas_report.values
        account_map_campaign = {}
        campaign_map_app = {}
        for info in infos:
            AccountId = info[0]
            CampaignId = info[1]
            map_app = pandas_conf.loc[pandas_conf['CampaignId'] == CampaignId][['AppId', 'Times']]
            if map_app.empty:
                #没查到的
                if AccountId in account_map_campaign:
                    account_map_campaign[AccountId].append(CampaignId)
                else:
                    account_map_campaign[AccountId] = [CampaignId]
            else:
                app_id = map_app['AppId'].values[0]
                times = map_app['Times'].values[0]
                if app_id is np.nan and times < MAX_TIMES:
                    #查到为空的
                    if AccountId in account_map_campaign:
                        account_map_campaign[AccountId].append(CampaignId)
                    else:
                        account_map_campaign[AccountId] = [CampaignId]
                else:
                    index = pandas_report.loc[pandas_report['CampaignId'] == CampaignId].index
                    pandas_report.loc[index, 'AppId'] = app_id
        logger.debug('account_map_campaign get finish, {}'.format(len(account_map_campaign)))
        get_success = []
        if account_map_campaign:
            manager = multiprocessing.Manager()
            mcces = adwords_task.get('mcc')
            for mcc in mcces:
                client = get_adwords_client(mcc)
                customer_id_queue = multiprocessing.Queue()
                input_queue = manager.Queue()
                GetCustomerIDs(client, customer_id_queue, get_success)
                if not customer_id_queue.empty():
                    while True:
                        try:
                            customer_id = customer_id_queue.get(timeout=0.01)
                            if customer_id in account_map_campaign:
                                input_queue.put([customer_id, account_map_campaign.get(customer_id)])
                        except Empty:
                            break
                    get_succeeded = manager.Queue()
                    get_failed = manager.Queue()
                    queue_size = input_queue.qsize()
                    num_processes = min(queue_size, MAX_PROCESSES)
                    logger.debug('Retrieving %d reports with %d processes:' % (queue_size, num_processes))
                    # Start all the processes.
                    processes = [AppWorker(mcc, adwords_task, input_queue, get_succeeded, get_failed) for _
                                 in range(num_processes)]
                    for process in processes:
                        process.start()
                    for process in processes:
                        process.join()
                    logger.debug('mcc:{} Finished get app with the following results:'.format(mcc))
                    while True:
                        try:
                            success = get_succeeded.get(timeout=0.01)
                            get_success.append(success['customerId'])
                            campaign_map_app.update(success['campaign_map_app'])
                        except Empty:
                            break
                        logger.debug('App for CustomerId "%d" succeeded.' % success['customerId'])

                    while True:
                        try:
                            failure = get_failed.get(timeout=0.01)
                        except Empty:
                            break
                        logger.error('App for CustomerId "{}" campaignId "{}" failed with error message: {}.'.format(
                        failure['customerId'], failure['customerId'], failure['e']))

            logger.debug('campaign_map_app get finish, {}'.format(len(campaign_map_app)))
            for campaign, app in campaign_map_app.iteritems():
                if not app:
                    continue
                index = pandas_report.loc[pandas_report['CampaignId'] == campaign].index
                pandas_report.loc[index, 'AppId'] = app
        # 修改列名
        # a.rename(columns={'A': 'a', 'B': 'b', 'C': 'c'}, inplace=True)
        result_file = campaign_report.replace('_report_tmp.csv', '_app_report_{}.csv'.format(date_now))
        pandas_report = pandas_report.sort_values(by="CampaignId")
        pandas_report.to_csv(result_file, index=False, columns=['CampaignId', 'CampaignName', 'AccountId', 'AppId'])
        if campaign_map_app:
            update_app_csv(app_conf, campaign_map_app)
        else:
            logger.debug('campaign_map_app is null no need to update_app_csv')
        return True
    except Exception:
        raise

def generate_campaign_app_report(report_file, adwords_task):
    try:
        app_csv_file = os.path.join(conf_dir, 'adwords_app.csv')
        if not os.path.exists(app_csv_file):
            generate_campaign_app_conf(app_csv_file, adwords_task)
        return integration_app_info(report_file, app_csv_file, adwords_task)
    except Exception:
        raise

def generate_campaign_reports(adwords_task):
    report_file = generate_merged_report(adwords_task)
    result1 = True
    result2 = True
    if adwords_task.get('report_class') == 0:
        try:
            result1 = generate_campaign_report(report_file)
        except Exception, ex:
            logger.error('generate_campaign_report error;{}'.format(ex))
            result1 = False
    elif adwords_task.get('report_class') == 1:
        try:
            result2 = generate_campaign_app_report(report_file, adwords_task)
        except Exception, ex:
            logger.error('generate_campaign_app_report error;{}'.format(ex))
            result2 = False
    else:
        try:
            result1 = generate_campaign_report(report_file)
        except Exception, ex:
            logger.error('generate_campaign_report error;{}'.format(ex))
            result1 = False
        try:
            result2 = generate_campaign_app_report(report_file, adwords_task)
        except Exception, ex:
            logger.error('generate_campaign_app_report error;{}'.format(ex))
            result2 = False
    if result1 and result2:
        # os.remove(report_file)
        return True
    else:
        return False

def do_adwords_tasks(adwords_tasks):
    logger.info('work_dir: {}'.format(work_dir))
    for adwords_task in adwords_tasks:
        logger.info('{} task begin'.format(adwords_task.get('task_type')))
        if adwords_task.get('task_type') == 'location':
            result = generate_location_report(adwords_task)
        elif adwords_task.get('task_type') == 'campaign':
            result = generate_campaign_reports(adwords_task)
        else:
            logger.error('{} task not support'.format(adwords_task.get('task_type')))
            continue
        if result:
            logger.info('{} task success end'.format(adwords_task.get('task_type')))
        else:
            logger.error('{} task fail end'.format(adwords_task.get('task_type')))



def main():
    do_adwords_tasks(adwords_tasks)
    # generate_merged_report(adwords_tasks[0])
    # parallel_report_downloads('./20180815112302', adwords_tasks[0])
    # generate_campaign_app_conf(r'./conf/app.csv', adwords_tasks[0])
    # generate_location_csv(r'./AdWords API Location Criteria 2018-07-02.csv')
    # integration_geo_info(r'./adwords_location_2443476994_report_LAST_7_DAYS.csv')
    # integration_app_info(r'./20180813120124/report/adwords_campaign_report_tmp.csv', r'./conf/app.csv', adwords_tasks[0])
    # generate_campaign_report(r'./20180810193448/report/adwords_campaign_report_tmp.csv')
    # client = get_adwords_client('237-147-9138')
    # GetCustomerIDs(client, '5593669658')

if __name__ == '__main__':
    startTime = datetime.now()
    main()
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))