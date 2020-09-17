import pandas as pd
def get_neo4j_data_nodes(str_cypher):
    '''
    查询neo4j数据
    :param str_cypher: neo4j查询代码
    :return: nodes
    '''
    from neo4j import GraphDatabase

    neoj4_host = 'bolt://dataneo4j.xianghuanji.com:2021'
    username = 'riskwrite'
    password = 'dioqd11'
    driver = GraphDatabase.driver(neoj4_host, auth=(username, password))

    nodes = driver.session().run(str_cypher).data()
    return nodes


def get_user_phone_total_community_city(str_date):
    '''
    获取某天成交订单的，紧急联系人、注册电话号码之间的关联情况
    :param str_date:查询日期
    :return:返回 管理的dataframe
    '''
    str_cyper='''
    MATCH data=((n:order)--(:user_id)--(d:phone)--(m:user_id)-[:have]-(k:order))
    where  n.status in  ['3','4','5','6','8','10','26','27','28','29','30','31'] 
    and k.status in  ['3','4','5','6','8','10','26','27','28','29','30','31'] 
    and n.user_id<>m.user_id and d.phone_number<>''  and n.trade_no =~ '.*{str_date}.*'  
    RETURN data  ;
    '''.format(str_date=str_date)

    nodes=get_neo4j_data_nodes(str_cyper)

    ## 订单汇总
    trade_no_info_list = []
    for i in nodes:
        for j in i['data']:
            if 'trade_no' in str(j):
                trade_no_info_list.append(j)

    df_trade_no = pd.DataFrame(trade_no_info_list)
    df_trade_no = df_trade_no.drop_duplicates()

    ##关联关系 以及关联key
    user_id_list = []
    keys = []
    for i in nodes:
        temp = [j for j in i['data'] if '用户' in str(j)]
        user_id_list.append(temp)

        temp_phone = [j for j in i['data'] if '电话' in str(j)]
        keys.append(temp_phone)

    ##合并汇总
    df_relate_total = pd.DataFrame([user_id_list, keys]).T
    df_relate_total.columns = ['user_info', 'keys']
    df_relate_total['user_id_1'] = df_relate_total['user_info'].apply(lambda x: x[0]['user_id'])
    df_relate_total['user_id_2'] = df_relate_total['user_info'].apply(lambda x: x[1]['user_id'])
    df_relate_total['keys_value'] = df_relate_total['keys'].apply(lambda x: x[0]['phone_number'])
    df_relate_total = df_relate_total[['user_id_1', 'user_id_2', 'keys_value']].drop_duplicates()


    ##user_id与号码的关联信息
    df_temp_user_id_1=df_relate_total[['user_id_1','keys_value']]
    df_temp_user_id_1 = df_temp_user_id_1.rename(columns={'user_id_1': 'user_id'})
    df_temp_user_id_2=df_relate_total[['user_id_2','keys_value']]
    df_temp_user_id_2 = df_temp_user_id_2.rename(columns={'user_id_2': 'user_id'})
    df_user_id_key_total=pd.concat([df_temp_user_id_1,df_temp_user_id_2]).drop_duplicates()

    group=df_user_id_key_total['keys_value'].groupby(df_user_id_key_total['user_id']).apply('_'.join)
    group=pd.DataFrame(group)
    group['user_id']=group.index
    group=group.reset_index(drop=True)
    df_user_id_key_total=group.copy()
    del group

    ##关联群体
    df_relate_total_1 = df_relate_total[['user_id_1', 'user_id_2']].drop_duplicates()

    import networkx as nx
    Graph = nx.from_pandas_edgelist(df_relate_total_1, 'user_id_1', 'user_id_2')
    for index, row in df_relate_total_1.iterrows():
        Graph.add_edge(row['user_id_1'], row['user_id_2'])
    community = nx.algorithms.components.connected_components(Graph)
    community = list(community)

    ##群体相关订单
    df_community_total = pd.DataFrame()
    index = 0
    for i in community:
        print(i)
        df_temp = df_trade_no[df_trade_no.user_id.isin(list(i))]
        df_temp['community_no'] = index
        index = index + 1
        df_community_total = pd.concat([df_community_total, df_temp])

    #合并关联信息
    try:
        df_community_total=df_community_total.merge(df_user_id_key_total,how='left',on='user_id')
    except:
        df_community_total=pd.DataFrame(columns=['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name', 'reg_phone',
       'stock_phone', 'risk_user_id', 'user_id', 'name',   'keys_value'])

    df_community_total=df_community_total[['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name'
       , 'user_id','keys_value']]
    df_community_total=df_community_total.fillna('')

    return df_community_total


def get_stock_phone_with_reg_or_emergency_community(str_date):
    '''
    获取发货电话号码与注册电话号码或者紧急联系人的关系
    :param str_date: 查询日期
    :return: df
    '''

    str_cyper='''
    MATCH data=((:user_id)-[:have]-(n:order)-[:stock_phone]-(d:phone)--(m:user_id)-[:have]-(k:order))
    where  n.status in  ['3','4','5','6','8','10','26','27','28','29','30','31'] 
    and n.user_id<>m.user_id and d.phone_number<>''  and n.trade_no =~ '.*{str_date}.*'
    and k.status in  ['3','4','5','6','8','10','26','27','28','29','30','31'] 
    RETURN data  ;
    '''.format(str_date=str_date)

    nodes=get_neo4j_data_nodes(str_cyper)

    ## 订单汇总
    trade_no_info_list = []
    for i in nodes:
        for j in i['data']:
            if 'trade_no' in str(j):
                trade_no_info_list.append(j)

    df_trade_no = pd.DataFrame(trade_no_info_list)
    df_trade_no = df_trade_no.drop_duplicates()

    ##关联关系 以及关联key
    user_id_list = []
    keys = []
    for i in nodes:
        temp = [j for j in i['data'] if '用户' in str(j)]
        user_id_list.append(temp)

        temp_phone = [j for j in i['data'] if '电话' in str(j)]
        keys.append(temp_phone)

    df_relate_total = pd.DataFrame([user_id_list, keys]).T
    df_relate_total.columns = ['user_info', 'keys']
    df_relate_total['user_id_1'] = df_relate_total['user_info'].apply(lambda x: x[0]['user_id'])
    df_relate_total['user_id_2'] = df_relate_total['user_info'].apply(lambda x: x[1]['user_id'])
    df_relate_total['keys_value'] = df_relate_total['keys'].apply(lambda x: x[0]['phone_number'])
    df_relate_total = df_relate_total[['user_id_1', 'user_id_2', 'keys_value']].drop_duplicates()

    ##user_id与号码的关联信息
    df_temp_user_id_1=df_relate_total[['user_id_1','keys_value']]
    df_temp_user_id_1 = df_temp_user_id_1.rename(columns={'user_id_1': 'user_id'})
    df_temp_user_id_2=df_relate_total[['user_id_2','keys_value']]
    df_temp_user_id_2 = df_temp_user_id_2.rename(columns={'user_id_2': 'user_id'})
    df_user_id_key_total=pd.concat([df_temp_user_id_1,df_temp_user_id_2]).drop_duplicates()

    group=df_user_id_key_total['keys_value'].groupby(df_user_id_key_total['user_id']).apply('_'.join)
    group=pd.DataFrame(group)
    group['user_id']=group.index
    group=group.reset_index(drop=True)
    df_user_id_key_total=group.copy()
    del group

    ##关联群体
    df_relate_total_1 = df_relate_total[['user_id_1', 'user_id_2']].drop_duplicates()

    import networkx as nx
    Graph = nx.from_pandas_edgelist(df_relate_total_1, 'user_id_1', 'user_id_2')
    for index, row in df_relate_total_1.iterrows():
        Graph.add_edge(row['user_id_1'], row['user_id_2'])
    community = nx.algorithms.components.connected_components(Graph)
    community = list(community)

    ##群体相关订单
    df_community_total = pd.DataFrame()
    index = 0
    for i in community:
        print(i)
        df_temp = df_trade_no[df_trade_no.user_id.isin(list(i))]
        df_temp['community_no'] = index
        index = index + 1
        df_community_total = pd.concat([df_community_total, df_temp])

    #合并关联信息
    try:
        df_community_total=df_community_total.merge(df_user_id_key_total,how='left',on='user_id')
    except:
        df_community_total=pd.DataFrame(columns=['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name', 'reg_phone',
       'stock_phone', 'risk_user_id', 'user_id', 'name',   'keys_value'])

    df_community_total=df_community_total[['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name'
       , 'user_id','keys_value']]
    df_community_total=df_community_total.fillna('')

    return df_community_total



def get_stock_phone_with_stock_phone_community(str_date):
    '''
    获取发货电话号码关联关系
    :param str_date: 查询日期
    :return: df
    '''

    str_cyper='''
    MATCH data=((:user_id)-[:have]-(n:order)-[:stock_phone]-(d:phone)-[:stock_phone]-(m:order)-[:have]-(:user_id))
    where  n.status in  ['3','4','5','6','8','10','26','27','28','29','30','31'] and n.user_id<>m.user_id and d.phone_number<>'' 
    and n.trade_no =~ '.*{str_date}.*' 
    and m.status in  ['3','4','5','6','8','10','26','27','28','29','30','31']
    RETURN data  ;
    '''.format(str_date=str_date)

    nodes=get_neo4j_data_nodes(str_cyper)

    ## 订单汇总
    trade_no_info_list = []
    for i in nodes:
        for j in i['data']:
            if 'trade_no' in str(j):
                trade_no_info_list.append(j)

    df_trade_no = pd.DataFrame(trade_no_info_list)
    df_trade_no = df_trade_no.drop_duplicates()

    ##关联关系 以及关联key
    user_id_list = []
    keys = []
    for i in nodes:
        temp = [j for j in i['data'] if '用户' in str(j)]
        user_id_list.append(temp)

        temp_phone = [j for j in i['data'] if '电话' in str(j)]
        keys.append(temp_phone)

    df_relate_total = pd.DataFrame([user_id_list, keys]).T
    df_relate_total.columns = ['user_info', 'keys']
    df_relate_total['user_id_1'] = df_relate_total['user_info'].apply(lambda x: x[0]['user_id'])
    df_relate_total['user_id_2'] = df_relate_total['user_info'].apply(lambda x: x[1]['user_id'])
    df_relate_total['keys_value'] = df_relate_total['keys'].apply(lambda x: x[0]['phone_number'])
    df_relate_total = df_relate_total[['user_id_1', 'user_id_2', 'keys_value']].drop_duplicates()

    ##user_id与号码的关联信息
    df_temp_user_id_1=df_relate_total[['user_id_1','keys_value']]
    df_temp_user_id_1 = df_temp_user_id_1.rename(columns={'user_id_1': 'user_id'})
    df_temp_user_id_2=df_relate_total[['user_id_2','keys_value']]
    df_temp_user_id_2 = df_temp_user_id_2.rename(columns={'user_id_2': 'user_id'})
    df_user_id_key_total=pd.concat([df_temp_user_id_1,df_temp_user_id_2]).drop_duplicates()

    group=df_user_id_key_total['keys_value'].groupby(df_user_id_key_total['user_id']).apply('_'.join)
    group=pd.DataFrame(group)
    group['user_id']=group.index
    group=group.reset_index(drop=True)
    df_user_id_key_total=group.copy()
    del group

    ##关联群体
    df_relate_total_1 = df_relate_total[['user_id_1', 'user_id_2']].drop_duplicates()

    import networkx as nx
    Graph = nx.from_pandas_edgelist(df_relate_total_1, 'user_id_1', 'user_id_2')
    for index, row in df_relate_total_1.iterrows():
        Graph.add_edge(row['user_id_1'], row['user_id_2'])
    community = nx.algorithms.components.connected_components(Graph)
    community = list(community)

    ##群体相关订单
    df_community_total = pd.DataFrame()
    index = 0
    for i in community:
        print(i)
        df_temp = df_trade_no[df_trade_no.user_id.isin(list(i))]
        df_temp['community_no'] = index
        index = index + 1
        df_community_total = pd.concat([df_community_total, df_temp])

    #合并关联信息
    try:
        df_community_total=df_community_total.merge(df_user_id_key_total,how='left',on='user_id')
    except:
        df_community_total=pd.DataFrame(columns=['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name', 'reg_phone',
       'stock_phone', 'risk_user_id', 'user_id', 'name',   'keys_value'])

    df_community_total=df_community_total[['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name'
       , 'user_id','keys_value']]
    df_community_total=df_community_total.fillna('')

    return df_community_total




def get_device_community(str_date):
    '''
    获取设备网络
    :param str_date: 查询日期
    :return: df
    '''

    str_cyper='''
    MATCH data=((:user_id)-[:have]-(n:order)-[:use_device]-(d:device)-[:use_device]-(m:order)-[:have]-(:user_id))
    where  n.status in ['3','4','5','6','8','10','26','27','28','29','30','31'] and n.user_id<>m.user_id and d.dev_id<>''
    and  n.trade_no =~ '.*{str_date}.*'
    RETURN data;
    '''.format(str_date=str_date)

    nodes=get_neo4j_data_nodes(str_cyper)

    ## 订单汇总
    trade_no_info_list = []
    for i in nodes:
        for j in i['data']:
            if 'trade_no' in str(j):
                trade_no_info_list.append(j)

    df_trade_no = pd.DataFrame(trade_no_info_list)
    df_trade_no = df_trade_no.drop_duplicates()

    ##关联关系 以及关联key
    user_id_list = []
    keys = []
    for i in nodes:
        temp = [j for j in i['data'] if '用户' in str(j)]
        user_id_list.append(temp)

        temp_phone = [j for j in i['data'] if '设备' in str(j)]
        keys.append(temp_phone)

    df_relate_total = pd.DataFrame([user_id_list, keys]).T
    df_relate_total.columns = ['user_info', 'keys']
    df_relate_total['user_id_1'] = df_relate_total['user_info'].apply(lambda x: x[0]['user_id'])
    df_relate_total['user_id_2'] = df_relate_total['user_info'].apply(lambda x: x[1]['user_id'])
    df_relate_total['keys_value'] = df_relate_total['keys'].apply(lambda x: x[0]['dev_id'])
    df_relate_total = df_relate_total[['user_id_1', 'user_id_2', 'keys_value']].drop_duplicates()

    ##user_id与号码的关联信息
    df_temp_user_id_1=df_relate_total[['user_id_1','keys_value']]
    df_temp_user_id_1 = df_temp_user_id_1.rename(columns={'user_id_1': 'user_id'})
    df_temp_user_id_2=df_relate_total[['user_id_2','keys_value']]
    df_temp_user_id_2 = df_temp_user_id_2.rename(columns={'user_id_2': 'user_id'})
    df_user_id_key_total=pd.concat([df_temp_user_id_1,df_temp_user_id_2]).drop_duplicates()

    group=df_user_id_key_total['keys_value'].groupby(df_user_id_key_total['user_id']).apply('_'.join)
    group=pd.DataFrame(group)
    group['user_id']=group.index
    group=group.reset_index(drop=True)
    df_user_id_key_total=group.copy()
    del group

    ##关联群体
    df_relate_total_1 = df_relate_total[['user_id_1', 'user_id_2']].drop_duplicates()

    import networkx as nx
    Graph = nx.from_pandas_edgelist(df_relate_total_1, 'user_id_1', 'user_id_2')
    for index, row in df_relate_total_1.iterrows():
        Graph.add_edge(row['user_id_1'], row['user_id_2'])
    community = nx.algorithms.components.connected_components(Graph)
    community = list(community)

    ##群体相关订单
    df_community_total = pd.DataFrame()
    index = 0
    for i in community:
        print(i)
        df_temp = df_trade_no[df_trade_no.user_id.isin(list(i))]
        df_temp['community_no'] = index
        index = index + 1
        df_community_total = pd.concat([df_community_total, df_temp])

    #合并关联信息
    try:
        df_community_total=df_community_total.merge(df_user_id_key_total,how='left',on='user_id')
    except:
        df_community_total=pd.DataFrame(columns=['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name', 'reg_phone',
       'stock_phone', 'risk_user_id', 'user_id', 'name',   'keys_value'])

    columns_list=['community_no','trade_no','status','comment','overdue_days','max_overduedays', 'user_name','user_id','keys_value']
    columns_list_left=set(list(df_community_total.columns)) & set(list(columns_list))
    columns_list_left=list(columns_list_left)
    columns_list_left.sort()

    df_community_total=df_community_total[columns_list_left]
    df_community_total=df_community_total.fillna('')

    return df_community_total