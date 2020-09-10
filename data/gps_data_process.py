import data.neo4j_data_explain as neo4j_data_explain
import pandas as pd


def get_map_data(str_date):
    '''
    查询某日所有聚集点数据
    :param str_date: 日期
    :return: 返回dic_df
    '''
    str_execute = '''
    MATCH data=((n:order)-[:gps_stock_label]-(d:gps_label)-[:gps_stock_label]-(m:order))
    where  n.status in ['3','4','5','6','8','10','26','27','28','29','30','31'] and n.user_id<>m.user_id  and  n.trade_no =~ '.*{date}.*'
    RETURN data
    '''.format(date=str_date)

    nodes=neo4j_data_explain.get_neo4j_data_nodes(str_execute)
    # 获取不同label下的order node
    dic_label = {}
    for i in range(len(nodes)):
        print(nodes[i]['data'][2].get('stock_gps_label'))
        label = nodes[i]['data'][2].get('stock_gps_label')
        if label in dic_label.keys():
            dic_label[label].append(nodes[i]['data'][0])
            dic_label[label].append(nodes[i]['data'][4])
        else:
            dic_label[label] = []

    # 获取不同label下的去重后的order node
    dic_label_set = dic_label.copy()


    # label下的order  node 转化为dataframe
    dic_label_df = {}
    for i in dic_label_set.keys():
        df_tmp = pd.DataFrame()
        trade_no_list = []
        overdue_days_list = []
        stock_gps_label_list = []
        latitude_list = []
        longitude_list = []
        status_list = []
        user_name_list = []
        max_overdue_day_list=[]

        for j in dic_label_set[i]:
            trade_no_list.append(j.get('trade_no'))
            overdue_days_list.append(j.get('overdue_days'))
            stock_gps_label_list.append(j.get('stock_gps_label'))
            latitude_list.append(j.get('latitude'))
            longitude_list.append(j.get('longitude'))
            status_list.append(j.get('status'))
            user_name_list.append(j.get('user_name'))
            max_overdue_day_list.append(j.get('max_overduedays'))
        df_tmp = pd.DataFrame([trade_no_list, overdue_days_list, stock_gps_label_list, latitude_list, longitude_list, status_list, user_name_list,max_overdue_day_list]).T
        df_tmp.columns = ['trade_no', 'overdue_days', 'stock_gps_label', 'latitude', 'longitude', 'status', 'user_name','max_overdue_day']
        df_tmp=df_tmp.drop_duplicates()
        dic_label_df[i] = df_tmp

    return dic_label_df



def df_dic_sta(dic_df):
    '''
    对不同标签下的label进行统计
    :param dic_df: df dic
    :return: df
    '''

    def sta_cal(dic_df,label):
        import numpy as np
        df_temp = dic_df[label]
        df_temp['status'] = df_temp.status.apply(lambda x: int(x))
        df_temp = df_temp[df_temp.status.isin([3, 4, 5, 6, 7, 10, 27, 28, 30])]
        df_temp_deal = df_temp[df_temp.status.isin([10, 27, 28, 30])]

        df_temp_deal=df_temp_deal.replace(np.nan, 0)
        df_temp_deal['overdue_days'] = df_temp_deal.overdue_days.apply(lambda x: int(x))
        df_temp_deal['max_overdue_day'] = df_temp_deal.max_overdue_day.apply(lambda x: int(x))

        df_temp_deal['Y']=np.where(df_temp_deal['overdue_days']>=30,1,0)
        df_temp_deal['Y_history'] = np.where(df_temp_deal['max_overdue_day'] >= 30, 1, 0)

        ##当前逾期七天及以上
        df_temp_deal['total_number_Y7'] = np.where(df_temp_deal.overdue_days >= 7, 1, 0)


        ##成交订单总数
        total_number=df_temp_deal.shape[0]

        ##当前逾期7天及以上
        total_number_Y7=df_temp_deal[df_temp_deal.total_number_Y7==1].shape[0]

        ##当前逾期30+订单总数
        total_number_Y1=df_temp_deal[df_temp_deal.Y==1].shape[0]

        ##历史最大逾期30+订单总数
        total_number_Y1_history = df_temp_deal[df_temp_deal.Y_history == 1].shape[0]

        ##当前逾期30+逾期率
        total_number_Y7_ratio = total_number_Y7 / total_number


        ##当前逾期30+逾期率
        total_number_Y1_ratio=total_number_Y1/total_number

        ##历史最大逾期30+逾期率
        total_number_Y1_history_ratio=total_number_Y1_history/total_number

        ##status 3 个数
        df_total=dic_df[label]
        str_status_3=str(','.join(df_total[df_total.status==3]['trade_no'].values))
        str_status_4 =str(','.join(df_total[df_total.status==4]['trade_no'].values))
        str_status_5 = str(','.join(df_total[df_total.status==5]['trade_no'].values))


        dic_result={}
        dic_result['total_number']=total_number
        dic_result['total_number_Y7']=total_number_Y7
        dic_result['total_number_Y30']=total_number_Y1
        dic_result['total_number_Y30_history']=total_number_Y1_history
        dic_result['total_number_Y7_ratio']=total_number_Y7_ratio
        dic_result['total_number_Y30_ratio']=total_number_Y1_ratio
        dic_result['total_number_Y30_history_ratio']=total_number_Y1_history_ratio
        dic_result['str_status_3']=str_status_3
        dic_result['str_status_4']=str_status_4
        dic_result['str_status_5']=str_status_5


        return  dic_result

    label_list = []

    total_number_list = []
    total_number_Y7_list = []
    total_number_Y30_list = []
    total_number_Y30_history_list = []
    total_number_Y7_ratio_list = []
    total_number_Y30_ratio_list = []
    total_number_Y30_history_ratio_list = []

    total_status_3_list=[]
    total_status_4_list=[]
    total_status_5_list=[]


    for i in dic_df.keys():
        label = i
        print('label:'+label)
        dic_result =sta_cal(dic_df,label)
        label_list.append(label)
        total_number_list.append(dic_result['total_number'])
        total_number_Y7_list.append(dic_result['total_number_Y7'])
        total_number_Y30_list.append(dic_result['total_number_Y30'])
        total_number_Y30_history_list.append(dic_result['total_number_Y30_history'])
        total_number_Y7_ratio_list.append(dic_result['total_number_Y7_ratio'])
        total_number_Y30_ratio_list.append(dic_result['total_number_Y30_ratio'])
        total_number_Y30_history_ratio_list.append(dic_result['total_number_Y30_history_ratio'])
        total_status_3_list.append(dic_result['str_status_3'])
        total_status_4_list.append(dic_result['str_status_4'])
        total_status_5_list.append(dic_result['str_status_5'])


    import pandas as pd
    df=pd.DataFrame([label_list,total_number_list,total_number_Y7_list,total_number_Y30_list,total_number_Y30_history_list,total_number_Y7_ratio_list,
                     total_number_Y30_ratio_list,total_number_Y30_history_ratio_list,total_status_3_list,total_status_4_list,total_status_5_list]).T
    df.columns=['label','成交订单数','DPD7+订单数','DPD30+订单数','历史最大30+订单数','DPD7_ratio','DPD30_raito','历史最大30+_ratio','待发货订单','拣货中订单','已发货订单']
    df['DPD7_ratio']=df['DPD7_ratio'].apply(lambda x:round(x,2))
    df['DPD30_raito']=df['DPD30_raito'].apply(lambda x:round(x,2))
    df['历史最大30+_ratio']=df['历史最大30+_ratio'].apply(lambda x:round(x,2))

    df=df.sort_values(by='DPD30_raito',ascending=False)
    return  df


def get_gps_label_map_data(dic_label_df,label):
    '''
        生成脚本
        :param dic_label_df: key为label,value为df的字典
        :param label: label
        :param path: 保存的路径
        :return:dic_gps_label_map_result
        '''
    df = dic_label_df[str(label)].copy()
    df = df[df.stock_gps_label == str(label)]
    df = df.fillna(0)
    df = df.drop_duplicates()
    list_latitude = list(df['latitude'])
    list_latitude = [float(i) for i in list_latitude]
    list_longitude = list(df['longitude'])
    list_longitude = [float(i) for i in list_longitude]
    label_list = ['status:' + str(i) + ',逾期天数：' + str(j) + ',姓名' + str(k) for i, j, k in
                  zip(list(df['status']), list(df['overdue_days']), list(df['user_name']))]
    label_list_1 = ['逾期天数：' + str(k) +
                    ' 订单号：' + str(i) +
                    ' 状态：' + str(j) + ' 用户名：' + str(l) + ' 最大逾期天数：' + str(m) for i, j, k, l, m in
                    zip(list(df['trade_no']), list(df['status']), list(df['overdue_days']), list(df['user_name']),
                        list(df['max_overdue_day']))]
    label_list_overduedays = [int(k) for k in list(df['overdue_days'])]
    label_list_max_overduedays = [int(k) for k in list(df['max_overdue_day'])]
    label_status_list = [int(k) for k in list(df['status'])]

    dic={}
    dic['lat_list']=list_latitude
    dic['long_list']=list_longitude
    dic['label_list']=label_list
    dic['label_list_1']=label_list_1
    dic['label_list_overduedays']=label_list_overduedays
    dic['label_list_max_overduedays']=label_list_max_overduedays
    dic['label_status_list']=label_status_list

    return dic