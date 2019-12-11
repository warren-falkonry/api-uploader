import os, requests, logging, json, time, urllib3, sys
from itertools import islice
from tools.job.manage import *

# Configuration Source File (see README for details)
configFilename="config.json"

with open('./config/'+configFilename, 'r') as ff:
  configdata=ff.read()
  config = json.loads(configdata)

###### Job Variables
accountID=int(config['accountID'])
datastreamID=int(config['datastreamID'])
serverURL=str(config['serverURL'])
apiToken=str(config['apiToken'])
entityCol=str(config['entityCol'])
timeFormat=str(config['timeFormat'])
timeZone=str(config['timeZone'])
timeIdentifier=str(config['timeIdentifier'])
injectEntity=bool(config['injectEntity'])
maxPendingJobs=int(config['maxPendingJobs'])

###### Check Variables are Set
for var in ['accountID','datastreamID','serverURL','apiToken','entityCol','timeFormat','timeZone','timeIdentifier','injectEntity']:
  if not globals()[var]:
    print(var+" has not been set")
    sys.exit()

###### Automatic variables
timestr = time.strftime("%Y%m%d-%H%M%S")
log='./logs/'+str(datastreamID)+'-'+timestr+'.log'
logging.basicConfig(filename=log,level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
input_file_directory="files-to-upload"
chunksize=10000
filesize=5000000
filesizemax=filesize-1

# Create INGEST endpoint
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
resp=create_job(apiToken, serverURL, accountID, datastreamID, 'INGESTDATA', entityCol, timeIdentifier, timeFormat, timeZone)

for input_file in os.listdir("./"+input_file_directory):
  print("Processing "+str(input_file))
  logging.info("JOB: Processing "+str(input_file))
  
  # Check for pending DIGEST jobs
  runningDigestJobs=get_jobs(apiToken, serverURL, accountID, datastreamID, 'DIGEST', 'CREATED')
  while len(runningDigestJobs) > maxPendingJobs:
    print("DIGEST jobs in pending status were found:")
    print(runningDigestJobs)
    print("Sleeping for 20 seconds before attempting resume...")
    time.sleep(20)
    runningDigestJobs=get_jobs(apiToken, serverURL, accountID, datastreamID, 'DIGEST', 'CREATED')
  i=0
  with open("./"+input_file_directory+'/'+input_file) as f:
    ###### Determine optimal chunksize for specified filesize
    print("Determining optimal number of lines for ~5MB chunksize...")
    logging.info("JOB: Determining optimal number of lines for ~5MB chunksize...")
    while filesize > filesizemax:
      with open("./"+input_file_directory+'/'+input_file) as f:
        head = list(islice(f, chunksize))
        testchunk = ''.join(head)
        testfile = open("checksize", "w")
        testfile.write(testchunk)
        testfile.close()
        filesize=os.path.getsize('checksize')
        if filesize > (filesizemax-1)/1000:
          chunksize=chunksize-100
      os.remove("checksize")
    print("Using chunksize=" + str(chunksize))
    logging.info("JOB: Using chunksize=" + str(chunksize))
    
    ##### Get CSV header
    with open("./"+input_file_directory+'/'+input_file) as f:
      header = f.readline()
      if injectEntity:
        header = header.strip('\n').strip('\r')+','+entityCol+'\n'
    
    ##### Process upload in chunks
    with open("./"+input_file_directory+'/'+input_file) as f:
      while True:
        next_n_lines = list(islice(f, chunksize))
        if not next_n_lines:
          break
        
        # Start INGEST job
        lines = [x.strip('\n').strip('\r') for x in next_n_lines]
        if injectEntity is True:
          payload = (','+entityCol+'\n').join(lines)+','+entityCol+'\n'
        else:
          payload = '\n'.join(lines)
        if i>0:
          payload=header+payload
        
        authTokenCSV = {"content-type":"text/csv", "Authorization" : "Bearer "+apiToken}
        r = requests.post(serverURL + resp['url'], data=payload, headers=authTokenCSV, verify=False)
        print("Chunk "+str(i)+" has been uploaded.")
        logging.info("JOB: Chunk "+str(i)+" has been uploaded.")
        i=i+1
  logging.info("JOB: Completed upload of "+str(input_file))
  
# End INGEST job
logging.info("JOB: Closing INGEST job")
endJob=update_job_status(apiToken, serverURL, accountID, [resp['linkid']], 'INGESTDATA', 'COMPLETED')