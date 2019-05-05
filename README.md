# FlumeToNiFi
Convert your Flume Agent configuratio to NiFi flows using a simple converter script. 

You need to pass 2 parameters, one for the location of your NiFi instance, and a second parameter for the location of your flume agent's config file you would like to convert. 

Running the script is simple:
```
python3 convert_flume_to_nifi.py --nifi-host=http://localhost:9090/nifi-api --filename=agent_config.conf
```

# Output
Once the Flume config file has been converted and pushed to NiFi, you will see the following (sample) appear on your canvas:

### Main Canvas
![alt text](https://github.com/willie-engelbrecht/FlumeToNiFi/blob/master/images/NiFi-img1.JPG "Sample NiFi Canvas")


### Sample Flume Config 1
![alt text](https://github.com/willie-engelbrecht/FlumeToNiFi/blob/master/images/NiFi-img3.JPG "Sample NiFi Agent Config 1")


### Sample Flume Config 2
![alt text](https://github.com/willie-engelbrecht/FlumeToNiFi/blob/master/images/NiFi-img2.JPG "Sample NiFi Agent Config 2")



## Prerequisites
* NiPyApi: https://pypi.org/project/nipyapi/

## Installing prerequisites
```
pip3 install nipyapi
```
