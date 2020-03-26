from http.server import BaseHTTPRequestHandler, HTTPServer
from hdbcli import dbapi
from urllib.parse import urlparse
import urllib
import sys
import requests
import io, os
import simplejson as json
import zipfile
import json
import re
import csv
import datetime

def onResponse(apiToken, surveyId, dataCenter):

    fileFormat = "csv"

    #Step 1 : Export Survey
    fileId = exportSurvey(apiToken,surveyId, dataCenter, fileFormat)

    #Step 2 : Parse file for records
    records = parseSurveyExport(fileId)

    #Step 3 : insert records in HANA DB
    writeRecordstoDB(records, surveyId)


def exportSurvey(apiToken, surveyId, dataCenter, fileFormat):

    surveyId = surveyId
    fileFormat = fileFormat
    dataCenter = dataCenter 

    # Setting static parameters
    requestCheckProgress = 0.0
    progressStatus = "inProgress"
    baseUrl = "https://{0}.qualtrics.com/API/v3/surveys/{1}/export-responses/".format(dataCenter, surveyId)
    headers = {
        "content-type": "application/json",
        "x-api-token": apiToken
    }

    #get Last timestamp
    startDate = getLastTimeStamp(surveyId)
    
    # Step 1: Creating Data Export , get Responses after the last timestamp
    downloadRequestUrl = baseUrl

    if startDate != '':
        downloadRequestPayload = '{"format":"' + fileFormat + '","useLabels":true,"startDate":"' + startDate + '"}'
    else:
        downloadRequestPayload = '{"format":"' + fileFormat + '","useLabels":true}'

    downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload, headers=headers)
    progressId = downloadRequestResponse.json()["result"]["progressId"]
    print(downloadRequestResponse.text)

    # Step 2: Checking on Data Export Progress and waiting until export is ready
    while progressStatus != "complete" and progressStatus != "failed":
        print ("progressStatus=", progressStatus)
        requestCheckUrl = baseUrl + progressId
        requestCheckResponse = requests.request("GET", requestCheckUrl, headers=headers)
        requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
        print("Download is " + str(requestCheckProgress) + " complete")
        progressStatus = requestCheckResponse.json()["result"]["status"]

    #step 2.1: Check for error
    if progressStatus is "failed":
        raise Exception("export failed")

    fileId = requestCheckResponse.json()["result"]["fileId"]

    # Step 3: Downloading file
    requestDownloadUrl = baseUrl + fileId + '/file'
    requestDownload = requests.request("GET", requestDownloadUrl, headers=headers, stream=True)

    # Step 4: Unzipping the file
    try:
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall("MyQualtricsDownload")
    except Exception as e:
        raise Exception("unzip failed" + e)

    fileName = requestDownload.headers['content-disposition']
    fileName = re.search('attachment; filename=(.+?).zip',fileName).group(1).replace("+"," ")

    return fileName

def parseSurveyExport(fileId):
    columnNumbers = {
                        "questionAnswerColumns":[],
                        "ResponseId": 0,
                        "managerID" : 0,
                        "employeeID" : 0,
                        "RecordedDate" : 0
                    }

    questions = []
    insertRecords = []
    
    with open("MyQualtricsDownload/" + fileId + ".csv") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader: 
            # get column numbers
            if line_count == 0:
                columnCount = len(row)
                for i in range(columnCount): 
                    if "SID" in row[i] or re.search("^Q(.*?)[0-9]", row[i]) :
                        columnNumbers["questionAnswerColumns"].append(i)
                    elif "RecordedDate" in row[i]:
                        columnNumbers["RecordedDate"] = i
                    elif "ResponseId" in row[i]:
                        columnNumbers["ResponseId"] = i
                    elif "Employee ID" in row[i]:
                        columnNumbers["employeeID"] = i
                    elif "Manager ID" in row[i]:
                        columnNumbers["managerID"] = i
                line_count += 1
            # get questions text
            elif line_count == 1:
                for columnNumber in columnNumbers["questionAnswerColumns"]:
                    question = {}
                    question["text"] = row[columnNumber]
                    question["columnNumber"] = columnNumber
                    questions.append(question)
                line_count += 1
            # get questions id
            elif line_count == 2:
                for q in questions:
                    q["id"] = re.search('{"ImportId":"(.+?)"}', row[q["columnNumber"]]).group(1)
                line_count += 1
            # get response records
            else:
                for q in questions:
                    record = {}
                    record["responseId"] = row[columnNumbers["ResponseId"]]
                    record["questionId"] = q["id"]
                    record["language"] = "en"
                    record["question"] = q["text"]
                    record["response"] = row[q["columnNumber"]]
                    if columnNumbers["managerID"] > 0:
                        record["managerId"] = row[columnNumbers["managerID"]]
                    else:
                        record["managerId"] = ''
                    if columnNumbers["employeeID"] > 0:
                        record["employeeID"] = row[columnNumbers["employeeID"]]
                    else:
                        record["employeeID"] = ''
                    record["responseDate"] = row[columnNumbers["RecordedDate"]]
                    insertRecords.append(record)
                line_count += 1
        print(f'Processed {line_count} lines.')
    
    return insertRecords

def writeRecordstoDB(records, surveyId):
    #Step 1 : Open connection to HDB
    conn = open_hdb_conn()

    #Step 2 : Owrite records to HDB
    if conn and conn.isconnected():
        print("connection to HDB open")
        conn.setautocommit(False)
        cursor = conn.cursor()
        for record in records:
            id = '"<SCHEMA>"."rid".NEXTVAL'
            values = id + ", '" + record["responseId"] + "', '" + record["questionId"] + "', '" + record["language"] + "', '" + record["question"] + "', '" +  record["response"] + "', '" +  record["managerId"] + "', '" +  record["employeeID"] + "', '" +  record["responseDate"] + "', '" +  surveyId + "'"
            cursor.execute("INSERT INTO \"<SCHEMA>\".\"<TABLE>\" VALUES(" + values +")")
            conn.commit()
            rowcount = cursor.rowcount
            if rowcount == 1:
                print("record is updated")
    
    #Step 3 : close connection to HDB
    close_hdb_conn(conn)

def getLastTimeStamp(surveyId):
    #Step 1 : Open connection to HDB
    conn = open_hdb_conn()

    #Step 2 : Get latest timestamp
    startDateforExportString = ''
    if conn and conn.isconnected():
        sql = "SELECT TOP 1 \"RESPONSEDATE\" FROM \"<SCHEMA>\".\"<TABLE>\" as \"response\" where \"SURVEYID\"='" + surveyId + "' " + 'order by "response"."RESPONSEDATE" desc'
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        if row and len(row) == 1:
            lastResponseDate = row[0]
            startDateforExport = lastResponseDate + datetime.timedelta(0,1)
            startDateforExportString = startDateforExport.strftime("%Y-%m-%dT%H:%M:%SZ")
            
    #Step 3 : close connection to HDB
    close_hdb_conn(conn)

    return startDateforExportString

def open_hdb_conn():
    print("opening connection to HDB")
    try:
        conn = dbapi.connect(address="<DB Host>", encrypt="true", port="<DB port>", user="<DB user>", sslValidateCertificate='false', password="<pwd>")
    except Exception as e:
        raise Exception("Open connection failed" + e)

    return conn

def close_hdb_conn(conn):
    if conn:
        try:
            conn.close()
            print("connection to HDB closed")
        except Exception as e:
            if conn and not conn.isconnected():
                print("connection to HDB closed")
            
def getReponse(d, dataCenter, apiToken):
    responseId = d['ResponseID']
    surveyId = d['SurveyID']
    
    headers = {
        "content-type": "application/json",
        "x-api-token": apiToken,
       }

    url = "https://{0}.qualtrics.com/API/v3/surveys/{1}/responses/{2}".format(dataCenter, surveyId, responseId)

    
    rsp = requests.get(url, headers=headers)
    print(rsp.json())

def parsey(c):
    x=c.decode().split("&")
    d = {}
    for i in x:
        a,b = i.split("=")
        d[a] = b

    d['CompletedDate'] = urllib.parse.unquote(d['CompletedDate'])

    return d

class Handler(BaseHTTPRequestHandler):

  # POST
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        d = parsey(post_data)
        surveyId = d['SurveyID']

        try:
            apiToken = "<Qualtrics API key>"
            dataCenter = "<Qualtrics data center>"
           
        except KeyError:
            print("set environment variables APIKEY and DATACENTER")
            sys.exit(2)
        

        #import all responses for survey and write to database
        #onResponse(apiToken, surveyId, dataCenter)

        #get single response 
        getReponse(d, dataCenter, apiToken)
 
if __name__ == '__main__':
    
    print('starting server...')
    server_address = ('0.0.0.0', 8080)
 
    httpd = HTTPServer(server_address, Handler)
    print('running server...')
    httpd.serve_forever()
