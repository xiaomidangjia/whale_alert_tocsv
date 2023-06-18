#鲸鱼异动报警
import time
from pprint import pprint
import pandas as pd
import numpy as np
import datetime,time
# For formatted dictionary printing>>>
from whalealert.whalealert import WhaleAlert
whale=WhaleAlert()# Specify a single transaction from the last 10 minutes>>>
import telegram
from dingtalkchatbot.chatbot import DingtalkChatbot
webhook = 'https://oapi.dingtalk.com/robot/send?access_token=69d2f134c31ced0426894ed975f29b519c1a8bd163a808840ef5812c5a0477a1'
xiaoding = DingtalkChatbot(webhook)
bot = telegram.Bot(token='6219784883:AAE3YXlXvxNArWJu-0qKpKlhm4KaTSHcqpw')
api_key='I38poa9dJRyy8qK8fG2KmSGicjXLjlLU'
s = 0
df = pd.DataFrame()
# transfer，mint
while True:
    time.sleep(1)
    s += 1
    print(s)
    if s%180 == 0:
        hash_data = pd.read_csv('hash_data.csv')
        hash_list = list(hash_data['hash'])
        #print(hash_list)
        start_time=int(time.time()-600)
        success,transactions,status=whale.get_transactions(start_time,api_key=api_key,min_value = 5000000)
        if success:
            #print(transactions)
            if len(transactions) == 0:
                continue
            else:
                for i in range(len(transactions)):
                    data = transactions[i]
                    blockchain = data['blockchain']
                    currecy = data['symbol']
                    transaction_type = data['transaction_type']
                    if blockchain in ('bitcoin','ethereum','tron') and currecy in ('BTC','ETH','USDT','USDC') and transaction_type == 'transfer':
                        hash_value = data['hash']
                        from_address = data['from']['address']
                        from_address_owner = data['from']['owner']
                        to_address = data['to']['address']
                        to_address_owner = data['to']['owner']
                        to_address_owner_type = data['to']['owner_type']
                        timestamp = data['timestamp']
                        time_local = time.localtime(timestamp)
                        dt = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
                        amount = data['amount']
                        amount_usd = data['amount_usd']
                        df = pd.concat([df,pd.DataFrame({'flag':s,'blockchain':blockchain,'currecy':currecy,'hash_value':hash_value,'from_address':from_address,'from_address_owner':from_address_owner,'to_address':to_address,'to_address_owner':to_address_owner,'to_address_owner_type':to_address_owner_type,'timestamp':dt,'amount':amount,'amount_usd':amount_usd},index=[0])])
                if len(df) == 0:
                    continue
                else:
                    logo = np.max(df['flag'])
                    sub_df = df[df.flag==logo]
                    sub_df = sub_df.reset_index(drop=True)
                    print(sub_df)
                    if len(sub_df) == 0:
                        continue
                    else:
                        sub_hash = []
                        now_time = str(datetime.datetime.now())[0:19]
                        for j in range(len(sub_df)):
                            if sub_df['from_address_owner'][j] == '' and sub_df['to_address_owner'][j] != '' and sub_df['to_address_owner_type'][j] == 'exchange':
                                #向telegram进行报警
                                blockchain = sub_df['blockchain'][j]
                                currecy_now = sub_df['currecy'][j]
                                if currecy_now in ('BTC','ETH'):
                                    from_address_now = sub_df['from_address'][j]
                                    to_address_now = sub_df['to_address'][j]
                                    to_address_owner_now = sub_df['to_address_owner'][j]
                                    utctime_now = sub_df['timestamp'][j] - datetime.timedelta(hours=8)
                                    amount_now = str(round(sub_df['amount'][j],2))
                                    amount_usd_now = str(round(sub_df['amount_usd'][j]/10000,1))
                                    hash_v = sub_df['hash_value'][j]
                                    if hash_v in hash_list:
                                        continue 
                                    else:
                                        sub_hash.append(hash_v)
                                        df = pd.read_csv('res_alert.csv')
                                        sub_df = pd.DataFrame({'date':utctime_now,'crypto':currecy_now,'exchange':to_address_owner_now,'number':amount_now,'value':amount_usd_now,'hash':hash_v},index=[0])
                                        df = pd.concat([df,sub_df])
                                        df.to_csv('res_alert.csv',index=False)

                                else:
                                    from_address_now = sub_df['from_address'][j]
                                    to_address_now = sub_df['to_address'][j]
                                    to_address_owner_now = sub_df['to_address_owner'][j]
                                    utctime_now = sub_df['timestamp'][j] - datetime.timedelta(hours=8)
                                    amount_now = str(round(sub_df['amount'][j]/10000,1))
                                    amount_usd_now = str(round(sub_df['amount_usd'][j]/10000,1))
                                    hash_v = sub_df['hash_value'][j]
                                    if hash_v in hash_list:
                                        continue 
                                    else:
                                        sub_hash.append(hash_v)
                                        df = pd.read_csv('res_alert.csv')
                                        sub_df = pd.DataFrame({'date':utctime_now,'crypto':currecy_now,'exchange':to_address_owner_now,'number':amount_now,'value':amount_usd_now,'hash':hash_v},index=[0])
                                        df = pd.concat([df,sub_df])
                                        df.to_csv('res_alert.csv',index=False)


                            else:
                                continue
                        #print(sub_hash)        
                        if len(sub_hash) > 0:
                            sub_hash_df = pd.DataFrame({'hash':sub_hash})
                            hash_data = pd.concat([hash_data,sub_hash_df])
                            #print(hash_data)
                            hash_data.to_csv('hash_data.csv',index=False)
                                
                            
    else:
        continue;