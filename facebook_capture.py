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
from Queue import Empty
from datetime import datetime
import os
import multiprocessing
import pandas as pd
import numpy as np
import time
import copy
import argparse
ACT = 'act_'
SLEEP_TIME = 5
BACKOFF_FACTOR = 5
MAX_TIMES = 3
MAX_RETRIES = 5
date_now = datetime.now().strftime("%Y%m%d%H%M%S")
# date_now = '20180815205050'
CAMPAIGN_APP_REPORT = 'facebook_campaign_app_report_{}.csv'.format(date_now)
CAMPAIGN_GEO_REPORT = 'facebook_campaign_geo_report_{}.csv'.format(date_now)
CAMPAIGN_NO_GEO_REPORT = 'facebook_campaign_no_geo_report_{}.csv'.format(date_now)
current_dir = os.path.abspath(os.path.dirname(__file__))
work_dir = os.path.join(current_dir, 'facebook_'+date_now)
report_dir = os.path.join(work_dir, 'report')
conf_dir = os.path.join(current_dir, 'conf')
facebook_app_conf  = os.path.join(conf_dir, 'facebook_app.csv')

MAX_PROCESSES = multiprocessing.cpu_count()

import os
import logging
import logging.handlers


logger = None

format_dict = {
    logging.DEBUG: logging.Formatter('%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'),
    logging.INFO: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.WARNING: logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'),
    logging.ERROR: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s'),
    logging.CRITICAL: logging.Formatter('%(asctime)s %(name)s %(funcName)s %(levelname)s %(message)s')
}

class Logger():
    __cur_logger = logging.getLogger()
    def __init__(self,loglevel):
        #set name and loglevel
        new_logger = logging.getLogger(__name__)
        new_logger.setLevel(loglevel)
        formatter = format_dict[loglevel]
        filehandler = logging.handlers.RotatingFileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'facebook.log'), mode='a')
        filehandler.setFormatter(formatter)
        new_logger.addHandler(filehandler)
        #create handle for stdout
        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)
        #add handle to new_logger
        new_logger.addHandler(streamhandler)
        Logger.__cur_logger = new_logger

    @classmethod
    def getlogger(cls):
        return cls.__cur_logger

logger = Logger(logging.DEBUG).getlogger()

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
                    input = copy.copy(result)
                    if task_type == 'campaign_geo':
                        input['CountryCode'] = AdsInsights.get('country')
                    input['CampaignId'] = AdsInsights.get('campaign_id')
                    input['Date'] = AdsInsights.get('date_start')
                    input['Impressions'] = AdsInsights.get('impressions')
                    input['Clicks'] = AdsInsights.get('clicks')
                    input['Spend'] = AdsInsights.get('spend')
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
            else:
                logger.debug('Report failed for {} with code {} after {} retries.'.format(result, e.code, retry_count))
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
        while True:
            try:
                logger.debug('{} app input_queue: {}'.format(self.ident, self.task_queue.qsize()))
                info = self.task_queue.get(timeout=0.01)
            except Empty:
                if not self.event.is_set():
                    time.sleep(SLEEP_TIME)
                    continue
                else:
                    logger.error('[{}] event have set and task_queue null'.format(self.ident))
                    break
            try:
                _DownloadReport(self.ident, info, self.input_queue, self.failure_queue)
            except Exception, ex:
                logger.error('info:{} error,{}'.format(info[0], ex))

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
    except Exception, ex:
        logger.error('get_account_campaign error:{}'.format(ex))

def get_campaign_app(result_queue, campaign_info, app_id):
    try:
        logger.debug('campaign:{} get app'.format(campaign_info.get('CampaignId')))
        facebook_app = facebook_apps.get(app_id)
        api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
        Campaign(api=api)
        promoted_object = Campaign(campaign_info.get('CampaignId')).get_ad_sets(fields=['promoted_object'], params={'limit': 1000})
        campaign_info['FBAppId'] = np.nan
        campaign_info['AppStoreUrl'] = np.nan
        for po in promoted_object:
            # try:
            if po.get('promoted_object') and po.get('promoted_object').get('object_store_url') and po.get('promoted_object').get('application_id'):
                campaign_info['FBAppId'] = po['promoted_object']['application_id']
                campaign_info['AppStoreUrl'] = po['promoted_object']['object_store_url']
                break
            # except:
            #     continue
        result_queue.put(campaign_info)
    except Exception, ex:
        logger.error('get_campaign_app error, ex:{}'.format(ex))

def generate_facebook_app_conf(app_campaign):
    if not os.path.exists(conf_dir):
        os.mkdir(conf_dir)
    csv_file_name = 'facebook_app.csv'
    app_csv_file = os.path.join(conf_dir, csv_file_name)
    manager = multiprocessing.Manager()
    result_queue_all = manager.Queue()
    results = []
    for app_id, app_campaign in app_campaign.iteritems():
        len_pool = min(len(app_campaign), MAX_PROCESSES)
        pool = multiprocessing.Pool(processes=len_pool)
        for data in app_campaign:
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
        pandas_conf.to_csv(app_csv_file, index=False, columns=['AccountId', 'CampaignId', 'FBAppId', 'AppStoreUrl', 'Times'])
        return results
    else:
        return []

def generate_campaign_app_report(datas):
    manager = multiprocessing.Manager()
    app_campaign = {}
    for app_id, basiness_account in datas.iteritems():
        result_queue = manager.Queue()
        results = []
        len_pool = min(len(basiness_account), MAX_PROCESSES)
        pool = multiprocessing.Pool(processes=len_pool)
        for b_a in basiness_account:
            pool.apply_async(get_account_campaign, (result_queue, b_a, app_id))
        pool.close()
        pool.join()
        if result_queue.qsize():
            while True:
                try:
                    result = result_queue.get(timeout=0.01)
                    results.append(result)
                except Empty:
                    break
        app_campaign[app_id] = results
    campaign_report_file = os.path.join(work_dir, CAMPAIGN_APP_REPORT)
    if not os.path.exists(facebook_app_conf):
        campaign_with_store = generate_facebook_app_conf(app_campaign)
        if campaign_with_store:
            if not os.path.exists(work_dir):
                os.mkdir(work_dir)
            pandas_conf = pd.DataFrame(campaign_with_store).sort_values(by="CampaignId")
            pandas_conf.to_csv(campaign_report_file, index=False, columns=['AccountId', 'CampaignId', 'CampaignName', 'FBAppId', 'AppStoreUrl'])
    else:
        integration_app_info(app_campaign, facebook_app_conf, campaign_report_file)
    return os.path.exists(campaign_report_file)

def update_app_csv(app_conf, campaign_map_app):
    try:
        logger.info('begin update_app_csv file')
        pandas_conf = pd.read_csv(app_conf, dtype=object)
        need_add = []
        flag_wr = False
        for campaign, datas in campaign_map_app.iteritems():
            map_app = pandas_conf.loc[pandas_conf['CampaignId'] == campaign][['FBAppId', 'AppStoreUrl', 'Times']]
            if map_app.empty:
                add = {'Times': 0}
                add['CampaignId'] = campaign
                add['FBAppId'] = datas[0]
                add['AppStoreUrl'] = datas[1]
                need_add.append(add)
            else:
                flag_wr = True
                if pd.notnull(datas[0]) or pd.notnull(datas[1]):
                    index = pandas_conf.loc[pandas_conf['CampaignId'] == campaign].index
                    pandas_conf.loc[index, ['FBAppId', 'AppStoreUrl', 'Times']] = [datas[0], datas[1], 0]
                else:
                    times = int(map_app['Times'].values[0])
                    index = pandas_conf.loc[pandas_conf['CampaignId'] == campaign].index
                    pandas_conf.loc[index, ['FBAppId', 'AppStoreUrl', 'Times']] = [datas[0], datas[1], times+1]
        if flag_wr:
            pandas_conf.to_csv(app_conf, index=False, mode='w', columns=['CampaignId', 'FBAppId', 'AppStoreUrl', 'Times'])
        if need_add:
            pandas_conf = pd.DataFrame(need_add)
            pandas_conf.to_csv(app_conf, index=False, mode='a', header=False, columns=['CampaignId', 'FBAppId', 'AppStoreUrl', 'Times'])
        logger.info('update_app_csv end')
        return True
    except Exception, ex:
        logger.error('update_app_csv: error {}'.format(ex))
        return False

def integration_app_info(app_campaign, app_conf, campaign_report_file):
    try:
        pandas_conf = pd.read_csv(app_conf, dtype=object)
        flag = False
        appid_map_campaign = {}
        for app, datas in app_campaign.iteritems():
            pandas_data = pd.DataFrame(datas)
            pandas_report = pandas_data[['AccountId', 'CampaignId', 'CampaignName']].drop_duplicates()
            pandas_report['FBAppId'] = np.nan
            pandas_report['AppStoreUrl'] = np.nan
            infos = pandas_report.values
            appid_map_campaign[app] = []
            for info in infos:
                CampaignId = info[1]
                map_app = pandas_conf.loc[pandas_conf['CampaignId'] == CampaignId][['FBAppId', 'AppStoreUrl', 'Times']]
                if map_app.empty:
                    #没查到的
                    if CampaignId in appid_map_campaign.get(app):
                        continue
                    else:
                        appid_map_campaign[app].append(CampaignId)
                else:
                    app_id = map_app['FBAppId'].values[0]
                    app_url = map_app['AppStoreUrl'].values[0]
                    times = int(map_app['Times'].values[0])
                    if (pd.isnull(app_id) or pd.isnull(app_url)) and times < MAX_TIMES:
                        #查到为空的
                        if CampaignId in appid_map_campaign.get(app):
                            continue
                        else:
                            appid_map_campaign[app].append(CampaignId)
                    else:
                        index = pandas_report.loc[pandas_report['CampaignId'] == CampaignId].index
                        pandas_report.loc[index, 'FBAppId'] = app_id
                        pandas_report.loc[index, 'AppStoreUrl'] = app_url
            logger.debug('appid_map_campaign get finish, {}'.format(len(appid_map_campaign)))
            manager = multiprocessing.Manager()
            campaign_map_app = {}
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
                            campaign_map_app[result.get('CampaignId')] = [result.get('FBAppId'), result.get('AppStoreUrl')]
                            index = pandas_report.loc[pandas_report['CampaignId'] == result.get('CampaignId')].index
                            pandas_report.loc[index, 'FBAppId'] = result.get('FBAppId')
                            pandas_report.loc[index, 'AppStoreUrl'] = result.get('AppStoreUrl')
                        except Empty:
                            break

            # 修改列名
            # a.rename(columns={'A': 'a', 'B': 'b', 'C': 'c'}, inplace=True)
            if not os.path.exists(work_dir):
                os.mkdir(work_dir)
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
            i_async_job = AdAccount(ACT+b_a.get('AccountId')).get_insights(is_async=True, params=report_define.get('params'), fields=report_define.get('fields'))
            task_queue.put((b_a, task_type, i_async_job))
            time.sleep(0.5)
    e.set()
    logger.debug('start_insights_task finish')

def get_campaign_report(datas, task_type):
    result_success = []
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
    logger.debug('{} Finished downloading reports'.format(task_type))
    while True:
        try:
            success = result_queue.get(timeout=0.01)
            result_success.append(success)
        except Empty:
            break
        logger.debug('Report for AccountId {} succeeded.'.format(success['AccountId']))
    logger.debug('download success {} report'.format(len(result_success)))
    n=0
    while True:
        try:
            fail = fail_queue.get(timeout=0.01)
            n+=1
        except Empty:
            break
        logger.debug('Report for AccountId {} failed.'.format(fail['AccountId']))
    logger.debug('download fail {} report'.format(n))
    return result_success


def generate_campaign_reports(datas, task_type):
    logger.info('generate_campaign_reports begin:{}'.format(task_type))
    result_success = get_campaign_report(datas, task_type)
    if result_success:
        if not os.path.exists(work_dir):
            os.mkdir(work_dir)
    else:
        logger.info('generate_campaign_reports get no data end:{}'.format(task_type))
        return False
    report_file = CAMPAIGN_GEO_REPORT if task_type == 'campaign_geo' else CAMPAIGN_NO_GEO_REPORT
    campaign_report_file = os.path.join(work_dir, report_file)
    pandas_data = pd.DataFrame(result_success)
    pandas_data = pandas_data.sort_values(by=['AccountId', 'Date'])
    if task_type == 'campaign_geo':
        pandas_data.to_csv(campaign_report_file, encoding='utf-8', index=False, mode='w', columns=['BusinessId', 'AccountId', 'CampaignId', 'Date', 'CountryCode', 'Clicks', 'Impressions', 'Results', 'Spend'])
    else:
        pandas_data.to_csv(campaign_report_file, encoding='utf-8', index=False, mode='w', columns=['BusinessId', 'AccountId', 'CampaignId', 'Date', 'Clicks', 'Impressions', 'Results', 'Spend'])
    return os.path.exists(campaign_report_file)

def get_accounts(result_queue, b_id, app_id):
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
                b_a['AccountId'] = a_id
                result_queue.put(b_a)
                n+=1
            flag = at.load_next_page()
        logger.debug('b_id:{} len at:{}'.format(b_id, n))
    except Exception, ex:
        logger.error('get_accounts error,ex:{}'.format(ex))
def get_business_users_all(api):
    bs = Business(fbid='me', api=api).get_business_users(params={'limit': 1000})
    business_ids = []
    flag = True
    while flag:
        for business in bs:
            b_id = business['business']['id']
            business_ids.append(b_id)
        flag = bs.load_next_page()
    return business_ids

def get_business_account_id(app_id):
    save_account_id = set()
    facebook_app = facebook_apps.get(app_id)
    api = FacebookAdsApi.init(facebook_app.app_id, facebook_app.app_secret, facebook_app.access_token)
    manager = multiprocessing.Manager()
    bs = get_business_users_all(api)
    # bs = bs[:8]
    logger.debug('app_id:{} bs get len:{}'.format(app_id, len(bs)))
    if not bs:
        return []
    result_queue = manager.Queue()
    len_pool = min(len(bs), MAX_PROCESSES)
    pool = multiprocessing.Pool(processes=len_pool)
    for b_id in bs:
        pool.apply_async(get_accounts, (result_queue, b_id, app_id))
    pool.close()
    pool.join()
    results = []
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

def do_facebook_tasks(facebook_tasks):
    logger.info('work_dir: {}'.format(work_dir))
    # import pdb;pdb.set_trace()
    apps = facebook_tasks.get('apps')
    datas = {}
    for app in apps:
        business_account = get_business_account_id(app)
        if not business_account:
            continue
        datas[app] = business_account
    if not datas:
        logger.error('get no data so exit')
        return False
    task_types = facebook_tasks.get('task_type')
    for facebook_task in task_types:
        logger.info('{} task begin'.format(facebook_task))
        if facebook_task == 'campaign_app':
            result = generate_campaign_app_report(datas)
        elif facebook_task in ['campaign_geo', 'campaign_no_geo']:
            result = generate_campaign_reports(datas, facebook_task)
        else:
            logger.error('{} task not support'.format(facebook_task))
            continue
        if result:
            logger.info('{} task success end'.format(facebook_task))
        else:
            logger.error('{} task fail end'.format(facebook_task))



def main():
    parser = argparse.ArgumentParser(description='Facebook Generate Report')
    parser.add_argument('-a', '--app_id', default='411977828960172',
                         help='list app_id')
    parser.add_argument('-t', '--type', required=True,
                         help='report type, must be campaign_app or campaign_geo or campaign_no_geo and join by ","')
    args = parser.parse_args()
    task_type = args.type.split(',')
    apps = args.app_id.split(',')
    facebook_tasks = {}
    facebook_tasks['task_type'] = task_type
    facebook_tasks['apps'] = apps
    do_facebook_tasks(facebook_tasks)

if __name__ == '__main__':
    startTime = datetime.now()
    main()
    endTime = datetime.now()
    logger.info('all seconds:{}'.format((endTime - startTime).seconds))