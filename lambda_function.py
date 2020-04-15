import random
import boto3 
from lxml import html
import time
from datetime import datetime
import requests

session = boto3.Session(
    aws_access_key_id=AKIAJNQ7APWT6FT3GBOA,
    aws_secret_access_key='''MIIEogIBAAKCAQEAide9185ZW5EYM5FDLVPWnORFNT8vbzOI/BfKW+H7YKVOQjZOde8aOscfEJeD
IQH9jTYUFlgiKCR/meYuXIw9x7X7sUgyGk4qFKmoO5dpFBhbpkIX7C7IxRTNn9NYnymrgEDdGDIK
DYUYCxIwqpHvAz3YaWq2DPzVOWZRrL4SC+0H/b6nzMxjkzT4KKK1nhOZbh3sz5+9mlz6qgpCP+Wx
vbbysWeDPCTNmPWIwjveZMcK9wt+GNvszpiwFPVviQKnyeES6bSkMNjGQremTH/tzfneJxbdZsm4
NN9HV1jkbIswDMVQJEytezrCF+5DoiYPhCitq3nR0Fs41CXrfKBd2wIDAQABAoIBAE2ENK+CzH+Q
5fJB82wvJJaQTFc4VD2N1rAl1Ne9Crd73rmffpoVv0NkrRSQj95lFyhtS/iQ0YyPke6DInlLKIcB
0SwONJbUdBewnPn+GMqDNC8YJgnc+WzTWRtTKBhBv79dOribk0mtGitHrHSPosDI68XEi+Jb4LDp
2G5zaQSpC7Mb2G5mvrvnyw08wTLlJGRzWi5FoeB9QIWJz8//AJ+K3xV+OuwNj95l72QT5jNfACbF
rDpGO0n4+XPG2HBRrQPmfBSCIiAWDiRikK6Z2FjxvtFsmYPV4cxkLG5S7DtRQDtZ0aBs4G+FInT+
lI1a7q2bTMDg1/0uG8pxvRqQBpECgYEA6/zTwvMyMlvGr59r0HBi8BLI45v3bL0fGGnM7QIt1sRS
dzR/JTk4TcsVjgYscPGQ/Wiso2IJdB8MC8c1rlnm7hlj7JZXPW7VWqBZ/LOfv9zj1uWMS3IFm3ig
XGKkXa+JfGSl2TDANIdCcbV/0RlPVwPL6Aai6AV9RxIJLXwMmDMCgYEAlYg9DJSJHiGGpM0mJumI
PW4zHRIC/WCyUJ6fFWS8ZpoKFoCdX7KGnSGOcnVMRFAo1nfGpMHEHeX06EmNL2sJ7xgivSd4gDbF
EDhQcYppAUBx7zNKtvZOK1keGv+2XPn4NaSCtE4PdPE8dnZcEjEnfX1+aOJH6frWGjVvc/mjG7kC
gYBFvL47gzDXTuXFKoBq5XtnZLW3BXU1ziGtTj6/33/6UqPoFDxqnKE15Ajnoo1phwmIyXETCzqt
9SrTJDiJ4Ils3VX5KWh4gBNOp8pP8ikIQteVvtoZxYVAr4H8Ky5VfTVM7FVSfKSgIUSSY8d03Fzk
4vtUAvoLROwDn6HUri7unwKBgAkvAdjY8oboNjW+573yD8z+DKOXbJmVK75pz4ln025VvZLeOUwc
Ucoqum3rKFD1v1xUpN2PjPTtH5p4kfQ8lbKaf9+wIeV196pkWwRuJL7P729qbgdIn0poQcIUvGV/
cLASt3hNZwOeoUEBSexQGoXJhE76vsHvBuE7MOolhc/5AoGAG5rOk4P5AAtj7qFg/MDtpBlTh1d5
pyb7w1YS0r//eeCLipodxzZU+SS02+wqRwoNVhn4Zpn3GHkBNMbqQGJ7HgSI3Js27I4PUlQLZkPE
YxnS5gAth2VnHNTJzg47RtpZc8qTsZ88MRZIxStGdBRlKYo0WdQ7tpQ8cyp9aQjTfBQ=''',
    
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
    rand_tick=random.sample(ticker_lst,int(len(ticker_lst)/300))
    tb_name=upload_to_dynamodb(rand_tick)
    best,names=get_best(tb_name)
    return 'Among all these stocks you pick: {}, the best one is {}, because this stock has the highest increase between Close price and Open Price, which is {}'.format(names,list(best.keys())[0],list(best.values())[0])
if __name__ == "__main__":
   
    handler()
    
