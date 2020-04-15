import random
import boto3 
from lxml import html
import time
from datetime import datetime
import requests
def scrape(ticker):

    url = "http://finance.yahoo.com/quote/%s?p=%s"%(ticker,ticker)
    response = requests.get(url, verify=False)
    print ("Parsing %s"%(url))
    time.sleep(4)
    parser =html.fromstring(response.text)
    dt={}
    dt['ticker']=ticker
    dt['name']=parser.xpath('//h1/text()')[0]
    summary_table = parser.xpath('//div[contains(@data-test,"summary-table")]//tr')
    
    try:
        for i in summary_table:
            a=i.xpath('.//td//span/text()')
            if len(a)==2:
                dt[a[0]]=a[1]
            else:
                val=i.xpath('.//td/text()')[0]
                dt[a[0]]=val
        print('scrape successfully for {}'.format(ticker))
        return dt
    
    except:
        print ("Failed to parse json response")
        return {"error":"Failed to parse json response"}

def upload_to_dynamodb(ticker_lst):
    db=boto3.resource('dynamodb')
    date=datetime.today().strftime('%Y_%m_%d')
    tb_name='stock_{}'.format(date)
    table = db.create_table(
        TableName=tb_name,
        KeySchema=[
            {
                'AttributeName': 'ticker',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'ticker',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    time.sleep(10)

    for i in ticker_lst:
        data=scrape(i)
        table.put_item(Item={i:j for i,j in data.items()})
    return tb_name

def get_best(tb_name):
    db=boto3.resource('dynamodb')
    table=db.Table(tb_name)
    all_stock=table.scan()['Items']
    names=[i['name'] for i in all_stock]
    difference=list(map(lambda x:{x['name']:float(x['Open'])-float(x['Previous Close'])}, table.scan()['Items']))
    best=[i for i in difference if list(i.values())[0]==max([list(i.values())[0] for i in difference])][0]
    return best,names

def handler(event=None,context=None):
    ticker_lst=[]
    with open('stock.txt','r') as f:
        for line in f:
            try:
                ticker=line.split('|')[0]
                ticker_lst.append(ticker)
            except:
                pass
    rand_tick=random.sample(ticker_lst,int(len(ticker_lst)/300))
    tb_name=upload_to_dynamodb(rand_tick)
    best,names=get_best(tb_name)
    return 'Among all these stocks you pick: {}, the best one is {}, because this stock has the highest increase between Close price and Open Price, which is {}'.format(names,list(best.keys())[0],list(best.values())[0])