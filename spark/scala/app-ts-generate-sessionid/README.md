**Description**

Given a time series data which is a click stream of user activity is 
stored in any flat files,ask is to enrich the data with sessionId

Session Definition

Session expires after inactivity of 60 minutes,because of inactivity no clickstream record will be generated.
Session remainsactive for total duration of 2 hours 

Steps:

Load data in any flat file format.
Read the data and use spark batch to do computation
