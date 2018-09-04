#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/8/16 15:13
# @Author  : long.zhang
# @Contact : long.zhang@opg.global
# @Site    : 
# @File    : facebook_capture.py
# @Software: PyCharm
# @Desc    :
from facebook_business.adobjects.campaign import Campaign
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.business import Business
from facebook_business.adobjects.businessuser import BusinessUser
from facebook_business.adobjects.adaccountuser import AdAccountUser as AdUser

# from facebook_conf import facebook_tasks, facebook_apps, report_defines
from facebook_conf import facebook_apps, report_defines
from logger import Logger
from Queue import Empty
from datetime import datetime
import logging.handlers
import os
import multiprocessing
import pandas as pd
import numpy as np
import time
import argparse
import psutil
from retrying import retry
from requests.exceptions import ConnectionError


ACT = 'act_'
SLEEP_TIME = 5
BACKOFF_FACTOR = 5
MAX_TIMES = 3
MAX_RETRIES = 5
date_now = datetime.now().strftime("%Y%m%d%H%M%S")
check_file_name = 'facebook_{}.csv'.format(date_now[:8])
# date_now = '20180827164346'
NEED_TOKEN_CAMPAIGN_REPORT = False
MONEY_FACTOR = 1000000
CAMPAIGN_APP_REPORT = 'facebook_campaign_app_report_{}.csv'.format(date_now)
CAMPAIGN_GEO_REPORT = 'facebook_campaign_geo_report_{}.csv'.format(date_now)
CAMPAIGN_NO_GEO_REPORT = 'facebook_campaign_no_geo_report_{}.csv'.format(date_now)
current_dir = os.path.abspath(os.path.dirname(__file__))
work_dir = os.path.join(current_dir, 'facebook_'+date_now, 'report')
TOKEN_CAMPAIGN_REPORT = os.path.join(work_dir, 'facebook_token_campaign_report.csv')
conf_dir = os.path.join(current_dir, 'conf')
facebook_app_conf = os.path.join(conf_dir, 'facebook_app.csv')
check_file = os.path.join(current_dir, check_file_name)
PID = os.getpid()
MAX_PROCESSES = multiprocessing.cpu_count()

logger = Logger(logging.DEBUG, 'facebook.log').getlogger()

def retry_if_ConnectionError(exception):
    return isinstance(exception, ConnectionError)


def _DownloadReport(process_id, info, input_queue, fail_queue):
    retry_count = 0
    result = info[0]
    task_type = info[1]
    i_async_job = info[2]
    while True:
        try:
            logger.debug('[{}/{}] Loading info for {}'.format(process_id, retry_count, result))
            while True:
                job = i_async_job.remote_read()
                logger.debug('account_id:{} finish:{}'.format(result.get('AccountId'), job['async_percent_completion']))
                time.sleep(0.5)
                if job:
                    logger.debug('account_id:{} Done'.format(result.get('AccountId')))
                    break
            flag = True
            result_cursor = i_async_job.get_result(params={'limit': 1000})
            if not result_cursor:
                logger.debug('account_id:{} get nothing'.format(result.get('AccountId')))
                return
            while flag:
                for AdsInsights in result_cursor:
                    input = result.copy()
                    if task_type == 'campaign_geo':
                        input['CountryCode'] = AdsInsights.get('country')
                    input['CampaignId'] = str(AdsInsights.get('campaign_id'))
                    input['Date'] = AdsInsights.get('date_start')
                    input['Impressions'] = AdsInsights.get('impressions')
                    input['Clicks'] = AdsInsights.get('clicks')
                    input['Spend'] = int(float(AdsInsights.get('spend'))*MONEY_FACTOR)
                    unique_actions = AdsInsights.get('unique_actions', [])
                    input['Results'] = 0
                    for u_a in unique_actions:
                        if u_a.get('action_type') == 'mobile_app_install':
                            input['Results'] = u_a.get('value')
                            break
                    logger.debug('[{}/{}] input:{}'.format(process_id, retry_count, input))
                    input_queue.put(input)
                flag = result_cursor.load_next_page()
            return
        except Exception, e:
            if retry_count < MAX_RETRIES:
                retry_count += 1
                time.sleep(retry_count * BACKOFF_FACTOR)
                logger.debug('Report failed for {},e:{}'.format(result, e))
            else:
                logger.error('Report failed for {} with code {} after {} retries.'.format(result, e.code, retry_count))
                fail_queue.put(result)
                return


class InsightsWorker(multiprocessing.Process):
    def __init__(self, task_queue, input_queue, failure_queue, e):

        super(InsightsWorker, self).__init__()
        self.task_queue = task_queue
        self.input_queue = input_queue
        self.failure_queue = failure_queue
        self.event = e

    def run(self):
        n = 0
        while True:
            try:
                logger.debug('{} app input_queue: {}'.format(self.ident, self.task_queue.qsize()))
                info = self.task_queue.get(timeout=0.01)
            except Empty:
                if not self.event.is_set():
                    time.sleep(SLEEP_TIME)
                    continue
                else:
                    logger.info('[{}] event have set and task_queue null'.format(self.ident))
                    break
            try:
                _DownloadReport(self.ident, info, self.input_queue, self.failure_queue)
                n+=1
                if not n%500:
                    logger.info('[{}] is running now,n:{}'.format(self.ident, n))
            except Exception, ex:
                logger.error('info:{} error,{}'.format(info[0], ex))


@retry(retry_on_exception=retry_if_ConnectionError, wait_fixed=5000)
def get_account_campaign(result_queue, business_account, app_id):
    try:
        facebook_app = facebook_apps.get(app_id)
        api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
        AdAccount(api=api)
        cp = AdAccount(ACT+business_account.get('AccountId')).get_campaigns(fields=[Campaign.Field.name], params={'limit': 1000})
        flag = True
        n = 0
        while flag:
            for campaign in cp:
                business_account['CampaignId'] = campaign['id']
                business_account['CampaignName'] = campaign['name']
                result_queue.put(business_account)
                n += 1
            flag = cp.load_next_page()
        logger.debug('a_id:{} len cp:{}'.format(business_account.get('AccountId'), n))
    except ConnectionError:
        raise
    except Exception, ex:
        logger.error('get_account_campaign error:{}'.format(ex))


@retry(retry_on_exception=retry_if_ConnectionError, wait_fixed=5000)
def get_campaign_app(result_queue, campaign_info, app_id):
    try:
        logger.debug('campaign:{} get app'.format(campaign_info.get('CampaignId')))
        facebook_app = facebook_apps.get(app_id)
        api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
        Campaign(api=api)
        promoted_object = Campaign(campaign_info.get('CampaignId')).get_ad_sets(fields=['promoted_object'], params={'limit': 1000})
        campaign_info['FBAppId'] = np.nan
        campaign_info['AppStoreUrl'] = np.nan
        campaign_info['CampaignName'] = np.nan
        for po in promoted_object:
            # try:
            if po.get('promoted_object') and po.get('promoted_object').get('object_store_url') and po.get('promoted_object').get('application_id'):
                campaign_info['FBAppId'] = str(po['promoted_object']['application_id'])
                campaign_info['AppStoreUrl'] = po['promoted_object']['object_store_url']
                break
            # except:
            #     continue
        campaign_name = Campaign(campaign_info.get('CampaignId')).api_get(fields=[Campaign.Field.name], params={'limit': 1000})
        if campaign_name.get('name'):
            campaign_info['CampaignName'] = campaign_name['name']
        result_queue.put(campaign_info)
    except ConnectionError:
        raise
    except Exception, ex:
        logger.error('get_campaign_app error, ex:{}'.format(ex))


def generate_facebook_app_conf(app_campaign):
    if not os.path.exists(conf_dir):
        os.mkdir(conf_dir)
    csv_file_name = 'facebook_app.csv'
    app_csv_file = os.path.join(conf_dir, csv_file_name)
    manager = multiprocessing.Manager()
    result_queue_all = manager.Queue()
    results = list()
    for app_id, campaign_info in app_campaign.iteritems():
        len_pool = min(len(campaign_info), MAX_PROCESSES)
        pool = multiprocessing.Pool(processes=len_pool)
        for data in campaign_info:
            pool.apply_async(get_campaign_app, (result_queue_all, data, app_id))
        pool.close()
        pool.join()
    if result_queue_all.qsize():
        while True:
            try:
                result = result_queue_all.get(timeout=0.01)
                results.append(result)
            except Empty:
                break
    if results:
        pandas_conf = pd.DataFrame(results).sort_values(by="CampaignId")
        pandas_conf['Times'] = 0
        pandas_conf.to_csv(app_csv_file, encoding='utf-8', index=False, columns=['CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl', 'Times'])
        if os.path.exists(app_csv_file):
            logger.info('first generate app_csv_file success')
        else:
            logger.info('first generate app_csv_file file')
        return results
    else:
        return []

# def generate_campaign_app_report(datas):
#     manager = multiprocessing.Manager()
#     app_campaign = dict()
#     for app_id, basiness_account in datas.iteritems():
#         result_queue = manager.Queue()
#         results = list()
#         len_pool = min(len(basiness_account), MAX_PROCESSES)
#         pool = multiprocessing.Pool(processes=len_pool)
#         for b_a in basiness_account:
#             pool.apply_async(get_account_campaign, (result_queue, b_a, app_id))
#         pool.close()
#         pool.join()
#         if result_queue.qsize():
#             while True:
#                 try:
#                     result = result_queue.get(timeout=0.01)
#                     results.append(result)
#                 except Empty:
#                     break
#         app_campaign[app_id] = results
#     campaign_report_file = os.path.join(work_dir, CAMPAIGN_APP_REPORT)
#     if not os.path.exists(facebook_app_conf):
#         campaign_with_store = generate_facebook_app_conf(app_campaign)
#         if campaign_with_store:
#             if not os.path.exists(work_dir):
#                 os.mkdir(work_dir)
#             pandas_conf = pd.DataFrame(campaign_with_store).sort_values(by="CampaignId")
#             pandas_conf.to_csv(campaign_report_file, index=False, columns=['AccountId', 'CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl'])
#     else:
#         integration_app_info(app_campaign, facebook_app_conf, campaign_report_file)
#     return os.path.exists(campaign_report_file)


def generate_campaign_app_report(datas):
    logger.info('generate campaign app report begin')
    if task_status_check(check_file, 'campaign_app', 'success'):
        return True

    app_campaign = dict()
    if not os.path.exists(TOKEN_CAMPAIGN_REPORT):
        generate_campaign_reports(datas, 'campaign_no_geo')
        if not os.path.exists(TOKEN_CAMPAIGN_REPORT):
            logger.error('generate {} file fail'.format(TOKEN_CAMPAIGN_REPORT))
            return False
    pandas_token = pd.read_csv(TOKEN_CAMPAIGN_REPORT, dtype=str)
    pandas_token = pandas_token[['TokenId', 'AccountId', 'CampaignId']].drop_duplicates()
    tokens = pandas_token['TokenId'].drop_duplicates().values
    for token in tokens:
        map_campaign = pandas_token.loc[pandas_token['TokenId'] == token][['AccountId', 'CampaignId']]
        if map_campaign.empty:
            continue
        app_campaign[str(token)] = map_campaign.to_dict(orient='records')
    if not app_campaign:
        logger.error('app_campaign get no data')
        return False

    campaign_report_file = os.path.join(work_dir, CAMPAIGN_APP_REPORT)
    if not os.path.exists(facebook_app_conf):
        campaign_with_store = generate_facebook_app_conf(app_campaign)
        if campaign_with_store:
            if not os.path.exists(work_dir):
                os.makedirs(work_dir)
            pandas_conf = pd.DataFrame(campaign_with_store).sort_values(by="CampaignId")
            pandas_conf.to_csv(campaign_report_file, encoding='utf-8', index=False,
                               columns=['AccountId', 'CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl'])
        else:
            logger.error('generate_facebook_app_conf get data file')
    else:
        integration_app_info(app_campaign, facebook_app_conf, campaign_report_file)
    return os.path.exists(campaign_report_file)


def update_app_csv(app_conf, campaign_map_app):
    try:
        logger.info('begin update_app_csv file')
        pandas_conf = pd.read_csv(app_conf, dtype=str)
        need_add = list()
        flag_wr = False
        for campaign, datas in campaign_map_app.iteritems():
            map_app = pandas_conf.loc[pandas_conf['CampaignId'] == campaign][['FBAppId', 'AppStoreUrl', 'Times', 'CampaignName']]
            if map_app.empty:
                add = {'Times': 0}
                add['CampaignId'] = campaign
                add['FBAppId'] = datas[0]
                add['AppStoreUrl'] = datas[1]
                add['CampaignName'] = datas[2]
                need_add.append(add)
            else:
                flag_wr = True
                if pd.notnull(datas[0]) and pd.notnull(datas[1]):
                    index = pandas_conf.loc[pandas_conf['CampaignId'] == campaign].index
                    pandas_conf.loc[index, ['FBAppId', 'AppStoreUrl', 'Times', 'CampaignName']] = [datas[0], datas[1], 0, datas[2]]
                else:
                    times = int(map_app['Times'].values[0])
                    index = pandas_conf.loc[pandas_conf['CampaignId'] == campaign].index
                    pandas_conf.loc[index, ['FBAppId', 'AppStoreUrl', 'Times', 'CampaignName']] = [datas[0], datas[1], times+1, datas[2]]
        if flag_wr:
            pandas_conf.to_csv(app_conf, encoding='utf-8', index=False, mode='w', columns=['CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl', 'Times'])
        if need_add:
            pandas_conf = pd.DataFrame(need_add)
            pandas_conf.to_csv(app_conf, encoding='utf-8', index=False, mode='a', header=False, columns=['CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl', 'Times'])
        logger.info('update_app_csv end')
        return True
    except Exception, ex:
        logger.error('update_app_csv: error {} campaign'.format(ex, campaign))
        return False


def integration_app_info(app_campaign, app_conf, campaign_report_file):
    try:
        pandas_conf = pd.read_csv(app_conf, dtype=str)
        flag = False
        appid_map_campaign = dict()
        campaign_map_app = dict()
        for app, datas in app_campaign.iteritems():
            pandas_data = pd.DataFrame(datas)
            pandas_report = pandas_data[['AccountId', 'CampaignId']].drop_duplicates()
            pandas_report['FBAppId'] = np.nan
            pandas_report['AppStoreUrl'] = np.nan
            pandas_report['CampaignName'] = np.nan
            infos = pandas_report.values
            appid_map_campaign[app] = list()
            for info in infos:
                CampaignId = info[1]
                map_app = pandas_conf.loc[pandas_conf['CampaignId'] == CampaignId][['FBAppId', 'AppStoreUrl','CampaignName', 'Times']]
                if map_app.empty:
                    #没查到的
                    if CampaignId in appid_map_campaign.get(app):
                        continue
                    else:
                        appid_map_campaign[app].append(CampaignId)
                else:
                    app_id = map_app['FBAppId'].values[0]
                    campaign_name = map_app['CampaignName'].values[0]
                    app_url = map_app['AppStoreUrl'].values[0]
                    times = int(map_app['Times'].values[0])
                    if (pd.isnull(app_id) or pd.isnull(app_url) or pd.isnull(campaign_name)) and times < MAX_TIMES:
                        #查到为空的
                        if CampaignId in appid_map_campaign.get(app):
                            continue
                        else:
                            appid_map_campaign[app].append(CampaignId)
                    else:
                        index = pandas_report.loc[pandas_report['CampaignId'] == CampaignId].index
                        pandas_report.loc[index, ['FBAppId', 'CampaignName', 'AppStoreUrl']] = [app_id, campaign_name, app_url]
            logger.debug('appid_map_campaign get finish, {}'.format(len(appid_map_campaign)))
            manager = multiprocessing.Manager()
            for app_id, CampaignIds in appid_map_campaign.iteritems():
                logger.debug('app_id:{} get len campaign:{}'.format(app_id, len(CampaignIds)))
                customer_app_queue = manager.Queue()
                len_pool = min(len(app_campaign), MAX_PROCESSES)
                pool = multiprocessing.Pool(processes=len_pool)
                for campaign_id in CampaignIds:
                    cp = {'CampaignId': campaign_id}
                    pool.apply_async(get_campaign_app, (customer_app_queue, cp, app_id))
                pool.close()
                pool.join()
                if customer_app_queue.qsize():
                    while True:
                        try:
                            result = customer_app_queue.get(timeout=0.01)
                            campaign_map_app[result.get('CampaignId')] = [result.get('FBAppId'), result.get('AppStoreUrl'), result.get('CampaignName')]
                            index = pandas_report.loc[pandas_report['CampaignId'] == result.get('CampaignId')].index
                            pandas_report.loc[index, ['FBAppId', 'AppStoreUrl', 'CampaignName']] = [result.get('FBAppId'), result.get('AppStoreUrl'), result.get('CampaignName')]
                        except Empty:
                            break
            # 修改列名
            # a.rename(columns={'A': 'a', 'B': 'b', 'C': 'c'}, inplace=True)
            if not os.path.exists(work_dir):
                os.makedirs(work_dir)
            pandas_report = pandas_report.sort_values(by="CampaignId")
            if flag:
                pandas_report.to_csv(campaign_report_file, encoding='utf-8', index=False, mode='a', header=False, columns=['AccountId', 'CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl'])
            else:
                pandas_report.to_csv(campaign_report_file, encoding='utf-8', index=False, mode='w', columns=['AccountId', 'CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl'])
                flag = True
        if campaign_map_app:
            update_app_csv(app_conf, campaign_map_app)
        else:
            logger.debug('campaign_map_app is null no need to update_app_csv')
        return True
    except Exception:
        raise


def start_insights_task(task_queue, e, datas, task_type):
    report_define = report_defines.get(task_type)
    for app, business_account in datas.iteritems():
        facebook_app = facebook_apps.get(app)
        api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
        AdAccount(api=api)
        for b_a in business_account:
            b_a['TokenId'] = app
            i_async_job = AdAccount(ACT+b_a.get('AccountId')).get_insights(is_async=True, params=report_define.get('params'), fields=report_define.get('fields'))
            task_queue.put((b_a, task_type, i_async_job))
            time.sleep(0.5)
    e.set()
    logger.info('******************************  start_insights_task finish  ******************************')


def get_campaign_report(datas, task_type):
    result_success = list()
    manager = multiprocessing.Manager()
    task_queue = manager.Queue(maxsize=1000)
    result_queue = manager.Queue()
    fail_queue = manager.Queue()
    e = multiprocessing.Event()
    p1 = multiprocessing.Process(target=start_insights_task, args=(task_queue, e, datas, task_type))
    p1.start()
    processes = [InsightsWorker(task_queue, result_queue, fail_queue, e) for _ in range(4)]
    for process in processes:
        process.start()
    for process in processes:
        process.join()
    p1.join()
    logger.info('{} Finished downloading reports'.format(task_type))
    while True:
        try:
            success = result_queue.get(timeout=0.01)
            result_success.append(success)
        except Empty:
            break
        logger.debug('Report for AccountId {} CampaignId {} succeeded.'.format(success['AccountId'], success['CampaignId']))
    logger.info('download success {} report'.format(len(result_success)))

    n=0
    while True:
        try:
            fail = fail_queue.get(timeout=0.01)
            n+=1
        except Empty:
            break
        logger.error('Report for AccountId {} failed.'.format(fail['AccountId']))
    logger.info('download fail {} report'.format(n))
    return result_success


def generate_campaign_reports(datas, task_type):
    logger.info('generate_campaign_reports begin:{}'.format(task_type))
    if task_status_check(check_file, task_type, 'success'):
        return True
    result_success = get_campaign_report(datas, task_type)
    if result_success:
        if not os.path.exists(work_dir):
            os.makedirs(work_dir)
    else:
        logger.info('generate_campaign_reports get no data end:{}'.format(task_type))
        return False

    report_file = CAMPAIGN_GEO_REPORT if task_type == 'campaign_geo' else CAMPAIGN_NO_GEO_REPORT
    campaign_report_file = os.path.join(work_dir, report_file)
    pandas_data = pd.DataFrame(result_success)
    pandas_data = pandas_data.sort_values(by=['AccountId', 'Date'])
    if NEED_TOKEN_CAMPAIGN_REPORT and not os.path.exists(TOKEN_CAMPAIGN_REPORT):
        pandas_data_tmp = pandas_data[['TokenId', 'BusinessId', 'AccountId', 'CampaignId']].drop_duplicates()
        pandas_data_tmp.to_csv(TOKEN_CAMPAIGN_REPORT, encoding='utf-8', index=False, mode='w', columns=['TokenId', 'BusinessId', 'AccountId', 'CampaignId'])

    if task_type == 'campaign_geo':
        pandas_data.to_csv(campaign_report_file, encoding='utf-8', index=False, mode='w', columns=['BusinessId', 'AccountId', 'CampaignId', 'Date', 'CountryCode', 'Clicks', 'Impressions', 'Results', 'Spend'])
    else:
        pandas_data.to_csv(campaign_report_file, encoding='utf-8', index=False, mode='w', columns=['BusinessId', 'AccountId', 'CampaignId', 'Date', 'Clicks', 'Impressions', 'Results', 'Spend'])
    return os.path.exists(campaign_report_file)


def get_accounts(result_queue, b_id, app_id):
    try:
        retry_count = 0
        while True:
            try:
                logger.debug('b_id:{} begin'.format(b_id))
                n = 0
                facebook_app = facebook_apps.get(app_id)
                api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
                Business(fbid='me', api=api)
                at = Business(b_id).get_client_ad_accounts(params={'limit': 1000})
                flag = True
                while flag:
                    for account in at:
                        b_a = {'BusinessId': b_id}
                        a_id = account['account_id']
                        b_a['AccountId'] = str(a_id)
                        result_queue.put(b_a)
                        n+=1
                    flag = at.load_next_page()
                logger.debug('b_id:{} len at:{}'.format(b_id, n))
                return
            except ConnectionError:
                if retry_count<MAX_RETRIES:
                    time.sleep(SLEEP_TIME)
                    continue
                else:
                    raise
    except Exception, ex:
        logger.error('get_accounts error,ex:{}'.format(ex))


def get_business_users_all(api):
    try:
        bs = Business(fbid='me', api=api).get_business_users(params={'limit': 1000})
        business_ids = list()
        flag = True
        while flag:
            for business in bs:
                b_id = business['business']['id']
                business_ids.append(str(b_id))
            flag = bs.load_next_page()
        return business_ids
    except Exception:
        raise

@retry(retry_on_exception=retry_if_ConnectionError, wait_fixed=5000)
def get_business_account_id(app_id):
    try:
        save_account_id = set()
        facebook_app = facebook_apps.get(app_id)
        api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
        manager = multiprocessing.Manager()
        bs = get_business_users_all(api)
        bs = bs[:1]
        logger.debug('app_id:{} get bs len:{}'.format(app_id, len(bs)))
        if not bs:
            return []
        result_queue = manager.Queue()
        len_pool = min(len(bs), MAX_PROCESSES)
        pool = multiprocessing.Pool(processes=len_pool)
        for b_id in bs:
            pool.apply_async(get_accounts, (result_queue, b_id, app_id))
        pool.close()
        pool.join()
        results = list()
        while True:
            try:
                result = result_queue.get(timeout=0.01)
                if result.get('AccountId') in save_account_id:
                    logger.debug('AccountId:{} have save'.format(result.get('AccountId')))
                    continue
                else:
                    results.append(result)
                    save_account_id.add(result.get('AccountId'))
            except Empty:
                break
        logger.info('app:{} get business_account_id len:{}'.format(app_id, len(results)))
        return results
    except Exception:
        raise


def do_facebook_tasks(facebook_tasks):
    logger.info('work_dir: {}'.format(work_dir))
    apps = facebook_tasks.get('apps')
    task_types = facebook_tasks.get('task_type')

    all_success = True
    for facebook_task in task_types:
        if facebook_task == 'campaign_app':
            if task_status_check(check_file, 'campaign_app', 'success'):
                continue
            else:
                all_success = False
                break
        elif facebook_task in ['campaign_geo', 'campaign_no_geo']:
            if task_status_check(check_file, facebook_task, 'success'):
                continue
            else:
                all_success = False
                break
        else:
            continue
    if all_success:
        logger.info('all task get report already')
        return True
    datas = dict()
    for app in apps:
        business_account = get_business_account_id(app)
        if not business_account:
            continue
        business_account = business_account[:1]
        datas[app] = business_account
    if not datas:
        logger.error('get no business and account data so exit')
        return False
    logger.info('get business and account data end')

    for facebook_task in task_types:
        logger.info('{} task begin'.format(facebook_task))
        result = False
        count = 0
        if facebook_task == 'campaign_app':
            while not result and count<MAX_RETRIES:
                result = generate_campaign_app_report(datas)
                count+=1
            else:
                logger.info('generate_campaign_app_report finish,count:{}'.format(count))

        elif facebook_task in ['campaign_geo', 'campaign_no_geo']:
            while not result and count<MAX_RETRIES:
                result = generate_campaign_reports(datas, facebook_task)
                count+=1
            else:
                logger.info('generate_campaign_reports finish,count:{}'.format(count))

        else:
            logger.error('{} task not support'.format(facebook_task))
            continue
        if result:
            logger.info('{} task success end'.format(facebook_task))
            task_status_set(check_file, facebook_task, 'success')
        else:
            task_status_set(check_file, facebook_task, 'faild')
            logger.error('{} task fail end'.format(facebook_task))

def delete_running_status(check_file):
    try:
        if not os.path.exists(check_file):
            return True
        pandas_csv = pd.read_csv(check_file, dtype=str)
        indexs = pandas_csv.loc[pandas_csv['status'] == 'running'].index
        # indexs = pandas_csv.loc[(pandas_csv['status'] == 'running') & (pandas_csv['pid'] == str(PID))].index
        pandas_csv = pandas_csv.drop(indexs)
        pandas_csv.to_csv(check_file, index=False)
    except Exception, ex:
            logger.error('delete_running_status error:{}'.format(ex))
            return True

def task_status_set(check_file, report_type, status):
    try:
        task_info = dict()
        if not os.path.exists(check_file):
            task_info['pid'] = PID
            task_info['report_type'] = ''
            task_info['status'] = 'running'
            infos = [task_info]
            pandas_data = pd.DataFrame(infos)
            logger.error('check_file:{} is not exist,will create it'.format(check_file))
            pandas_data.to_csv(check_file, index=False)
        if status == 'success':
            pandas_csv = pd.read_csv(check_file, dtype=str)
            map_info = pandas_csv.loc[(pandas_csv['report_type'] == report_type) & (pandas_csv['status'] == status)]
            if not map_info.empty:
                return True
        task_info['pid'] = PID
        task_info['report_type'] = report_type
        task_info['status'] = status
        infos = [task_info]
        pandas_data = pd.DataFrame(infos)
        pandas_data.to_csv(check_file, index=False, mode='a', header=False)
        return os.path.exists(check_file)
    except Exception, ex:
            logger.error('task_status_set error:{}'.format(ex))
            return False


def task_status_check(check_file, report_type, status='success'):
    try:
        task_info = dict()
        task_info['pid'] = PID
        task_info['report_type'] = ''
        task_info['status'] = 'running'
        infos = [task_info]
        pandas_data = pd.DataFrame(infos)
        if not os.path.exists(check_file):
            logger.error('check_file:{} is not exist,will create it'.format(check_file))
            pandas_data.to_csv(check_file, index=False)
            return False
        else:
            pandas_csv = pd.read_csv(check_file, dtype=str)
            map_info = pandas_csv.loc[(pandas_csv['report_type'] == report_type) & (pandas_csv['status'] == status)]
            if not map_info.empty:
                logger.info('task_status_check report_type:{} have status:{} task'.format(report_type, status))
                return True
            else:
                logger.debug('task_status_check report_type:{} not have status:{} task'.format(report_type, status))
                return False

    except pd.errors.EmptyDataError:
            logger.error('check file is empty,need add infos')
            pandas_data.to_csv(check_file, index=False)
            return False

def single_task_check(check_file):
    try:
        task_info = dict()
        task_info['pid'] = PID
        task_info['report_type'] = ''
        task_info['status'] = 'running'
        infos = [task_info]
        pandas_data = pd.DataFrame(infos)
        if os.path.exists(check_file):
            pandas_csv = pd.read_csv(check_file, dtype=str)
            map_pid = pandas_csv.loc[pandas_csv['status'] == 'running'][['pid']]
            if not map_pid.empty:
                current_pids = psutil.pids()
                pids = map_pid['pid'].values
                for pid in pids:
                    if int(pid) in current_pids:
                        logger.error('pid:{} is running,so exit current program')
                        return False
        else:
            pandas_data.to_csv(check_file, index=False)
            return os.path.exists(check_file)

        pandas_data.to_csv(check_file, index=False, mode='a', header=False)
        return os.path.exists(check_file)

    except pd.errors.EmptyDataError:
        logger.error('check file is empty,so need write data')
        pandas_data.to_csv(check_file, index=False)
        return os.path.exists(check_file)

def main():
    parser = argparse.ArgumentParser(description='Facebook Generate Report')
    parser.add_argument('-a', '--app_id', default='411977828960172',
                        help='list app_id')
    parser.add_argument('-t', '--type', required=True,
                        help='report type, must be campaign_app or campaign_geo or campaign_no_geo and join by ","')
    args = parser.parse_args()
    if not single_task_check(check_file):
        return False
    task_type = args.type.split(',')
    task_type.sort(reverse=True)
    apps = args.app_id.split(',')

    facebook_tasks = dict()
    facebook_tasks['task_type'] = task_type
    facebook_tasks['apps'] = apps

    if 'campaign_app' in facebook_tasks.get('task_type'):
        global NEED_TOKEN_CAMPAIGN_REPORT
        NEED_TOKEN_CAMPAIGN_REPORT = True

    do_facebook_tasks(facebook_tasks)
    delete_running_status(check_file)
def main2():
    delete_running_status(r'D:\adwords\facebook_20180904.csv')


if __name__ == '__main__':
    startTime = datetime.now()
    main()
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))