import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_job(apiToken, serverURL, accountID, datastreamID, jobType, entityCol, timeIdentifier, timeFormat, timeZone):
  # Create Token Strings
  authToken = {"Authorization" : "Bearer "+apiToken}
  # Create Job Metadata
  dataValue = """
  {'jobType': '%s', "status": 'CREATED', 'datastream': '%s', 
  'spec': 
      { 'format': 
          {'entityIdentifier': '%s', 
          'timeIdentifier': '%s', 
          'timeFormat': '%s', 
          'timeZone': '%s'
          }
      }
  }""" %(jobType,datastreamID,entityCol,timeIdentifier,timeFormat,timeZone)
  r = requests.post(serverURL + "/api/1.1/accounts/%s/jobs" %(accountID), data=dataValue, headers=authToken, verify=False)
  rdict = r.json()
  # Get data loading linkID
  linkID = rdict["links"]
  url = linkID[0]["url"]
  print("data url:",serverURL + url)
  resp={'url':url, 'linkid':rdict['id']}
  return resp

def get_jobs(apiToken, serverURL, accountID, datastreamID, jobType, jobStatus):
  r = {}
  authToken = {"Authorization" : "Bearer "+apiToken}
  r = requests.get(serverURL + "/api/1.1/accounts/"+str(accountID)+"/jobs?limit=100&type="+jobType+"&status="+jobStatus+"&datastream="+str(datastreamID), headers=authToken, verify=False)
  rdict = r.json()
  ids=[]
  for k in rdict:
    ids.append(k['id'])
  return ids
 
def update_job_status(apiToken, serverURL, accountID, jobIds, jobType, jobStatus):  
  r = {}
  authToken = {"Authorization" : "Bearer "+apiToken}
  output=[]
  for id in jobIds:
    url = "/api/1.1/accounts/%s/jobs/%s" %(accountID,id)
    dataValue = {'jobType': jobType, 'status': jobStatus}
    r = requests.put(serverURL + url, data=str(dataValue), headers=authToken, verify=False)
    rinfo = r.json()
  return rinfo