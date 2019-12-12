# Python API Uploader

## Requirements
* Python 2 or 3
* requests module (e.g. `pip install requests`)
* Only tested on Linux (but "theoretically" should work with Python on Windows...)

## Basic Usage
### Configure `config.json` in the `config` folder
```
{
  "accountID": 12345671325988864,
  "datastreamID": 89101122513838080,
  "serverURL": "https://1.2.3.4",
  "apiToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE4ODE2MDM0NTUsICJlbWFpbCIgOiAicm9kcmlnby5henVhQHNnc2NtLmNsIiwgIm5hbWUiIDogInJvZHJpZ28uYXp1YUBzZ3NjbS5jbCIsICJzZNECUnnjdcjdkdoCKTAyMDgiIH0.ieBIRCnlVNQVvxDrZ0IcJ1X3vU5jsc86ll1fGbOKqnY",
  "entityCol": "SOMENAME",
  "injectEntity": "True",
  "timeFormat": "YYYY-M-DD H:m:s.SS",
  "timeZone": "America/Los_Angeles",
  "timeIdentifier": "time",
  "maxPendingJobs": 20
}
```
#### Special Config Definitions
* `entityCol` - The column header label for the entity (to be used if it exists, or created if it does not exist)
* `injectEntity` - `True` if the data does not have an entity column.  `False` if the data already has the entity column defined above
* `maxPendingJobs` - Intended for metering DIGEST jobs, but this is not currently supported due to inconsistent digest behavior with segmented ingest sessions.  May be enabled in the future, but it is ignored and can be omitted or left as-is.

### Running the Job
1. Copy the `api-loader` application to the server where you will run the job
2. Using a screen session (or nohup) is recommended
3. Place the CSV files to upload 
4. Run `python api_uploader.py` to start the upload process

### Logs
Log file is created for every job run and is located in the `logs` directory.  File name will follow a format of `datastreamID-YYYYMMDD-HHmmss.log`.  

e.g. `654466453039628288-20191211-153807.log`

Salient log entries are prefaced with `JOB` so a simpler tail of log file can be viewed by grepping for only JOB. 

For example:
```
tailf 654466453039628288-20191211-153807.log | grep JOB
```
