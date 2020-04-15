import random
import boto3 
from lxml import html
import time
from datetime import datetime
import requests
import smtplib

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    
)
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
    db=session.resource('dynamodb',region_name='us-east-1')
    date=datetime.today().strftime('%Y_%m_%d')
    tb_name='stock_{}'.format(date)
    boo=True
    while boo:
        try:
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
            boo= False
        except:
            table = db.Table(tb_name)
            table.delete()
            time.sleep(10)
        
    time.sleep(10)

    for i in ticker_lst:
        data=scrape(i)
        table.put_item(Item={i:j for i,j in data.items()})
    return tb_name

def get_best(tb_name):
    db=session.resource('dynamodb',region_name='us-east-1')
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
    rand_tick=random.sample(ticker_lst,50)
    tb_name=upload_to_dynamodb(rand_tick)
    best,names=get_best(tb_name)
    return ' We choose {} number of stock, the best one is {},difference is {},check dynamodb for more info'.format(len(names),list(best.keys())[0],list(best.values())[0])

def email():
   s = smtplib.SMTP(host='smtp.gmail.com', port=587)
   s.ehlo()
   s.starttls()
   s.ehlo()
   string=handler()
   s.login('', '')
   s.sendmail('wqeqsada2131@gmail.com', 'wqeqsada2131@gmail.com', 'Subject: \n {}'.format(string)) 
if __name__ == "__main__":
   
    email()
