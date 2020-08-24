import yfinance as yf
import flask.json
from flask import Flask, request, jsonify,redirect,url_for
from flask_cors import CORS
from datetime import date
import datetime
import pandas as pd
import numpy as np
import math
import boto3
from boto3 import resource
from collections import defaultdict
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import decimal
from flasgger import Swagger
from flasgger import swag_from


app = Flask(__name__)
swagger = Swagger(app)
dynamodb = boto3.resource('dynamodb')
cardinfotable = dynamodb.Table('CreditCard')
mocktable = dynamodb.Table('MockUp_Data')

client = boto3.resource('dynamodb')

CORS(app)

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(flask.json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

app.json_encoder = DecimalEncoder

dataresponse = mocktable.scan(
           # AttributesToGet=('amount','areix_category','currency_code','made_on'),
            FilterExpression=Attr("amount").lte(0)
            )


def get_cashrebate(category):
    cash_rebate = cardresponse['Item']['benefits'][category]['cash_rebate']
    if cash_rebate != "None":
        cash_rebate = cash_rebate.split('%')
        cash_rebate = float(cash_rebate[0])/100
        return cash_rebate
    elif cash_rebate == "None":
        cash_rebate = 0
        return cash_rebate

def get_mile(category):
    
    mile2moneyrate = 0.016
    mile = cardresponse['Item']['benefits'][category]['mile']
    if mile != "None" and "N/A":
        try:
            mile = mile.split("$")
            mile = mile[1].split("/mile")
            mile2money = mile2moneyrate/float(mile[0])
            return mile2money
        except:
            return 0
    else:
        mile2money = 0
        return mile2money

def calculation90(psid,product_id):
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    tdy = datetime.datetime.now()
    last = (datetime.date(tdy.year,tdy.month,tdy.day)-datetime.timedelta(90)).strftime('%Y-%m-%d')
    dataresponse90 = mocktable.query(
        KeyConditionExpression=Key('psid').eq(psid),       
        ProjectionExpression='amount, areix_category, currency_code, made_on',
        FilterExpression=Attr("amount").lte(0) & Attr('made_on').between(last, today))['Items']
    global cardresponse
    cardresponse = cardinfotable.get_item(
            Key={
                'product_id': product_id,
        })
    
    data = pd.DataFrame(dataresponse90)
    day = []
    week = []
    month = []
    quarter = []
    year = []

    for i in data['made_on']:
        day.append(i)
        a = i.split('-')
        y = a[0]
        year.append(y)
        m = date(int(y), int(a[1]), 1).strftime('%b')
        w = date(int(a[0]), int(a[1]), int(a[2])).isocalendar()[1]
        week.append('W'+str(w)+' '+m+' '+y)
        month.append(m + ' '+ y)
        quarter.append('Q'+str(math.ceil(int(a[1])/3))+' '+y)
        
    data['Week'],data['Date'],data['Month'],data['Quarter'],data['Year'],data['Cash Rebate'],data['Asia Mile ($)'],data['Total']=[week,day,month,quarter,year,np.nan,np.nan,np.nan]

        ######Calculating rewards
    for i in range(data.shape[0]):
        category = data.iloc[i]["areix_category"]

        #if data.iloc[i]["currency_code"] != "HKD":
         #   local = "Local"
        #else:
         #   local = "Overseas"
        
        data.at[i,'Cash Rebate'] = -(get_cashrebate(category)*float(data.iloc[i]["amount"]))
        data.at[i,'Asia Mile ($)'] = -(get_mile(category)*float(data.iloc[i]["amount"]))
        data.at[i,'Total'] = data.iloc[i]["Cash Rebate"] + data.iloc[i]["Asia Mile ($)"]
        
    datesorted_df = data.sort_values(by='Date')
    savedamount90 = 0
    for i in datesorted_df['Total']:
        savedamount90 += i
        
    return savedamount90

def calculation(psid,product_id):
    dataresponse = mocktable.query(
    KeyConditionExpression=Key('psid').eq(psid),
    ProjectionExpression='amount, areix_category, currency_code, made_on',
    FilterExpression=Attr("amount").lte(0))
    global cardresponse
    cardresponse = cardinfotable.get_item(
            Key={
                'product_id': product_id,
        }
        )
    data = pd.DataFrame(dataresponse['Items'])
    #data.drop(columns=['mode', 'psid','Created_at','account_id','account_name','category','mode','provider_name','status','transaction_id','updated_at','description'])

    day = []
    week = []
    month = []
    quarter = []
    year = []

    for i in data['made_on']:
        day.append(i)
        a = i.split('-')
        y = a[0]
        year.append(y)
        m = date(int(y), int(a[1]), 1).strftime('%b')
        w = date(int(a[0]), int(a[1]), int(a[2])).isocalendar()[1]
        week.append('W'+str(w)+' '+m+' '+y)
        month.append(m + ' '+ y)
        quarter.append('Q'+str(math.ceil(int(a[1])/3))+' '+y)
        
    data['Week'],data['Date'],data['Month'],data['Quarter'],data['Year'],data['Cash Rebate'],data['Asia Mile ($)'],data['Total']=[week,day,month,quarter,year,np.nan,np.nan,np.nan]

        ######Calculating rewards
    for i in range(data.shape[0]):
        category = data.iloc[i]["areix_category"]

        #if data.iloc[i]["currency_code"] != "HKD":
         #   local = "Local"
        #else:
         #   local = "Overseas"
        
        data.at[i,'Cash Rebate'] = -(get_cashrebate(category)*float(data.iloc[i]["amount"]))
        data.at[i,'Asia Mile ($)'] = -(get_mile(category)*float(data.iloc[i]["amount"]))
        data.at[i,'Total'] = data.iloc[i]["Cash Rebate"] + data.iloc[i]["Asia Mile ($)"]
        
    datesorted_df = data.sort_values(by='Date')
    
    dodsavedamount = pd.pivot_table(data,index=['Date','areix_category'], values=['Total'],aggfunc=np.sum)
    dodsavedamount = dodsavedamount.to_dict()['Total']
    def dict_val():
        return {'Dining & Beverage':0.0,'Financials':0,'Healthcare':0.0,'Home':0.0,'Leisure':0.0,'Others':0.0,'Shopping':0.0,'Transportation':0.0,'Sum':0.0}
    dodres = defaultdict(dict_val)
    for i,j in dodsavedamount.items():
        dodres[i[0]][i[1]] = j
        dodres[i[0]]['Sum'] += j    

    
    wowsavedamount = pd.pivot_table(data,index=['Week','areix_category'], values=['Total'],aggfunc=np.sum)
    wowsavedamount = wowsavedamount.to_dict()['Total']    
    wowres = defaultdict(dict_val)
    for i,j in wowsavedamount.items():
        wowres[i[0]][i[1]] = j
        wowres[i[0]]['Sum'] += j    

    
    momsavedamount = pd.pivot_table(data,index=['Month','areix_category'], values=['Total'],aggfunc=np.sum)
    momsavedamount = momsavedamount.to_dict()['Total']    
    momres = defaultdict(dict_val)
    for i,j in momsavedamount.items():
        momres[i[0]][i[1]] = j
        momres[i[0]]['Sum'] += j    

    
    qoqsavedamount = pd.pivot_table(data,index=['Quarter','areix_category'], values=['Total'],aggfunc=np.sum)
    qoqsavedamount = qoqsavedamount.to_dict()['Total']    
    qoqres = defaultdict(dict_val)
    for i,j in qoqsavedamount.items():
        qoqres[i[0]][i[1]] = j
        qoqres[i[0]]['Sum'] += j    

    
    yoysavedamount = pd.pivot_table(data,index=['Year','areix_category'], values=['Total'],aggfunc=np.sum)
    yoysavedamount = yoysavedamount.to_dict()['Total']    
    yoyres = defaultdict(dict_val)
    for i,j in yoysavedamount.items():
        yoyres[i[0]][i[1]] = j
        yoyres[i[0]]['Sum'] += j    
    output ={}
    output['dod'],output['wow'],output['mom'],output['qoq'],output['yoy'] = dodres,wowres,momres,qoqres,yoyres

    return output

#calculation90('69904d92-b1f1-11ea-81a7-0a7a347b3dd5','cchk222062')



@app.route('/')
def hello_world():
    return 'Hello, World!'

@swag_from('./apidocs/creditcardlist.yml')
@app.route('/creditcards/<psid>/', methods=["GET"])
def card_list(psid):

    try:
        response = cardinfotable.scan(
            AttributesToGet=("product_id","product_name","img_url","provider_name","conditions","new_comer_campaign")
        )
        creditcardlist = response['Items']
        
        for i in creditcardlist:
            i['saved_amount'] = calculation90(str(psid),str(i['product_id']))
        
        return {'error': False, 'success': True, 'data': creditcardlist,
                'msg': 'return credit card list'}
    except Exception as e:
        # traceback.print_exc()
        return {'error': True, 'success': False, 'data': None, 'msg': e.__str__()}

@swag_from('./apidocs/creditcardinfo.yml')
@app.route('/creditcards/<psid>/<product_id>', methods=["GET"])
def card_info(psid,product_id):
    login = request.args.get("login")

    try:
        response = cardinfotable.get_item(
            Key={
        'product_id': str(product_id),
        }
        )
        global cardinfo
        cardinfo = response['Item']
        a = calculation(str(psid),str(product_id))
        
        #####cal peer saved amount
        for i in range(len(cardinfo['peer'])):
            peer_savedamount = calculation90(str(psid),cardinfo['peer'][i]['product_id'])
            cardinfo['peer'][i]['saved_amount'] = peer_savedamount
            del cardinfo['peer'][i]['description']
            del cardinfo['peer'][i]['exrate']
            del cardinfo['peer'][i]['late_pay']
            del cardinfo['peer'][i]['min_pay']
            del cardinfo['peer'][i]['cash_APR']
            del cardinfo['peer'][i]['retail_APR']
        
        cardinfo.update(a)
        
        
        
        return {'error': False, 'success': True, 'data': cardinfo,
                'msg': 'return credit card list'}
    except Exception as e:
        # traceback.print_exc()
        return {'error': True, 'success': False, 'data': None, 'msg': e.__str__()}
    
    if login:
        print(login)
 
    else:
        pass



@swag_from('./apidocs/stock.yml')
@app.route('/stock/<symbol>', methods=["GET"])
def get_stock(symbol):
    """
    get the stock
    """
    if not symbol:
        return {"error": True, "success": False, "data": None, 'msg': 'please specify the symbol'}
    
    ### URL params
    page = request.args.get("page")
    print(page)

    ### The logic and database CRUD operations are performed in here

    try:
        res = {}
        yahoo_data = yf.Ticker(symbol)

        res['ma200'] = yahoo_data.info['twoHundredDayAverage']
        res['ma50'] = yahoo_data.info['fiftyDayAverage']
        res['price'] = yahoo_data.info['previousClose']
        res['forward_pe'] = yahoo_data.info['forwardPE']
        res['forward_eps'] = yahoo_data.info['forwardEps']
        if yahoo_data.info['dividendYield'] is not None:
            res['dividend_yield'] = yahoo_data.info['dividendYield'] * 100

        return {'error':False,'success':True,'data':res, 'msg':'return stock data'}
    
    except Exception as e:
        # traceback.print_exc()
        return {'error':True,'success':False,'data':None, 'msg':e.__str__()}

if __name__ == '__main__':
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    app.run(host='0.0.0.0')

