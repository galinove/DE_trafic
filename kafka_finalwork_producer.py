from kafka import KafkaProducer
import pandas as pd
import numpy
import json

sourcefile = pd.read_csv('/home/galinov/Downloads/trafic_07_2023.csv', sep='|', encoding='cp1251')

producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')

for i in range(sourcefile.shape[0]):
	subject = str(sourcefile.iat[i,0])
	org_type = str(sourcefile.iat[i,1])
	speed = str(sourcefile.iat[i,2])
	obj_code = str(sourcefile.iat[i,3])
	date = str(sourcefile.iat[i,4])
	vol_down = str(sourcefile.iat[i,5])
	vol_up = str(sourcefile.iat[i,6])
	
	message = {
         "subject": subject,
         "org_type": org_type,
	 "speed": speed,
	 "obj_code": obj_code,
	 "date": date,
	 "vol_down": vol_down,
	 "vol_up": vol_up
	}
	message = json.dumps(message)	
	producer.send('finaltask', value=message.encode('utf-8'))
producer.close()


