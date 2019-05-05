# FlumeToNiFi
Convert your Flume Agent configuratio to NiFi flows using a simple converter script. 

You need to pass 2 parameters, one for the location of your NiFi instance, and a second parameter for the location of your flume agent's config file you would like to convert. 

Running the script is simple:
```
python3 convert_flume_to_nifi.py --nifi-host=http://localhost:9090/nifi-api --filename=agent_config.conf
```

## Prerequisites
* NiPyApi: https://pypi.org/project/nipyapi/

## Installing prerequisites
```
pip3 install nipyapi
```
