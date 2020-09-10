from flask import Flask
from flask import request
from flask import render_template
from flask import session
import data.gps_data_process as gps_data_process
import data.neo4j_data_explain as neo4j_data_explain
import json

app = Flask(__name__)
app.config['SECRET_KEY'] = '123456'


@app.route('/')
def initl():
    if request.method=='GET':
        return render_template('gps_label_sta.html')

@app.route('/gps_label_query/',methods=['GET','POST'])
def gps_label_query():
    '''
    label统计查询
    :return:
    '''
    if request.method=='GET':
        if('2' not in str(session.get('date'))):
            from datetime import datetime,timedelta
            yesterday=datetime.now()+timedelta(days=-1)
            yesterday=yesterday.strftime(format('%Y%m%d'))
            return render_template('gps_label_sta.html',yesterday=yesterday)
        else:
            return render_template('gps_label_sta.html', yesterday=session.get('date'))
    else:
        print('hello')
        session.clear()
        date=request.form.get('gps_label_time')
        print('datetime:'+str(date))
        session['date']=date


        ##获取数据
        dic_label_df=gps_data_process.get_map_data(date)
        df_total_sta=gps_data_process.df_dic_sta(dic_label_df)
        df_total_sta.columns= ['label', 'deal_order_number', 'd7_order_number', 'd30_order_number', 'history_max_d30_order_number', 'd7_ratio', 'd30_ratio', 'history_max_d30_order_ratio', 'dfh_orders', 'jhz_orders', 'yfh_orders']
        df_total_sta['dfh_orders']= df_total_sta['dfh_orders'].apply(lambda x:str(x).replace(']','').replace('[','').replace('\'',""))
        df_total_sta['jhz_orders']= df_total_sta['jhz_orders'].apply(lambda x:str(x).replace(']','').replace('[','').replace('\'',""))
        df_total_sta['yfh_orders']= df_total_sta['yfh_orders'].apply(lambda x:str(x).replace(']','').replace('[','').replace('\'',""))
        df_total_sta=df_total_sta[['label', 'deal_order_number','d7_order_number', 'd30_order_number', 'history_max_d30_order_number', 'd7_ratio', 'd30_ratio', 'history_max_d30_order_ratio', 'dfh_orders', 'jhz_orders', 'yfh_orders']]

        ##格式转换
        data_total=[dict(row) for index,row in df_total_sta.iterrows()]
        import  json
        data_total = json.dumps(data_total)
        data_total.replace('\'',"")


        return render_template('gps_label_sta.html',data1=data_total,yesterday=date)


@app.route('/gps_label_detail_query/',methods=['GET','POST'])
def gps_label_detail_query():
    '''
    label 订单信息详情页
    :return:
    '''
    if request.method=='GET':
        return render_template('gps_label_sta.html')
    else:
        import json
        label=str(request.form.get('gps_label'))
        date=session.get('date')
        dic_label_df = gps_data_process.get_map_data(date)
        df_temp=dic_label_df[label]

        dic_detail_list=[dict(row) for index,row in df_temp.iterrows()]
        dic_detail_list = json.dumps(dic_detail_list)
        print(str(dic_detail_list))

    return  render_template('gps_label_detail.html',data1=dic_detail_list)


@app.route('/gps_label_map_load/',methods=['GET','POST'])
def gps_label_map_load():
    if request.method=='GET':
        return render_template('gps_label_sta.html')
    else:
        label = str(request.form.get('gps_label_map'))
        date=request.form.get('date')
        if((date=='') or (date is None)):
            date=session.get('date')
        print('label:'+str(label))
        print('date:'+str(date))
        ##获取地图数据
        dic_label_df = gps_data_process.get_map_data(date)
        dic_gps_label_map_result = gps_data_process.get_gps_label_map_data(dic_label_df,label)
        dic_gps_label_map_result = json.dumps(dic_gps_label_map_result)

        return  render_template('gps_label_map.html',dic_result=dic_gps_label_map_result)


@app.route('/neo4j_user_phone_query/',methods=['GET','POST'])
def neo4j_user_phone_query():
    '''
    用户紧急联系人与注册号码关联关系查询
    :return:
    '''
    if request.method=='GET':
        return render_template('gps_label_sta.html')
    else:
        import json
        date = request.form.get('neo4j_query_label_time')
        df_temp = neo4j_data_explain.get_user_phone_total_community_city(date)


        dic_detail_list = [dict(row) for index, row in df_temp.iterrows()]
        dic_detail_list = json.dumps(dic_detail_list)
        print(str(dic_detail_list))

        #cols 数据生成
        import pandas as pd
        df_columns = pd.DataFrame(df_temp.columns)
        df_columns.columns = ['field']
        df_columns['title']=df_columns.field
        #df_columns.loc[df_columns.field=='community_no','title']='团体编号'
        df_columns['sort']=True

        dic_col_list = [dict(row) for index, row in df_columns.iterrows()]
        dic_col_list = json.dumps(dic_col_list)
        print(str(dic_col_list))

    return render_template('neo4j_user_phone.html', data1=dic_detail_list,data_col=dic_col_list)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port='5006',debug=True)
