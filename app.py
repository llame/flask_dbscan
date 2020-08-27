from flask import Flask
from flask import request
from flask import render_template
from flask import session
import data.gps_data_process as gps_data_process


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
        import json
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

if __name__ == '__main__':
    app.run(host='127.0.0.1',port='5000')
