import os, requests, logging
import json, time
from itertools import islice
from manage_jobs import create_job, get_jobs, update_job_status
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

###### Job Variables
accountID=613022171325988864
datastreamID=653739899805024256
serverURL="https://200.54.255.130"
apiToken="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODE2MDM0NTUsICJlbWFpbCIgOiAicm9kcmlnby5henVhQHNnc2NtLmNsIiwgIm5hbWUiIDogInJvZHJpZ28uYXp1YUBzZ3NjbS5jbCIsICJzZXNzaW9uIiA6ICI2MTMwOTQyMzEwODc3MTAyMDgiIH0.ieBIRCnlVNQVvxDrZ0IcJ1X3vU5jsc86ll1fGbOKqnY"
entityCol = "CR011"
timeFormat="YYYY-M-DD H:m:s.SS"
timeZone="America/Los_Angeles"
timeIdentifier="time"
injectEntity=True
maxPendingJobs=20
r = {}

###### Automatic variables
timestr = time.strftime("%Y%m%d-%H%M%S")
log='./logs/'+str(datastreamID)+'-'+timestr+'.log'
logging.basicConfig(filename=log,level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
input_file_directory="files-to-upload"
chunksize=10000
filesize=5000000
filesizemax=filesize-1

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
  
  # Create INGEST endpoint
  resp=create_job(apiToken, serverURL, accountID, datastreamID, 'INGESTDATA', entityCol, timeIdentifier, timeFormat, timeZone)
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
  # End INGEST job
  logging.info("JOB: Closing INGEST job endpoint for chunk "+str(i))
  endJob=update_job_status(apiToken, serverURL, accountID, [resp['linkid']], 'INGESTDATA', 'COMPLETED')
  logging.info("JOB: Completed upload of "+str(input_file))