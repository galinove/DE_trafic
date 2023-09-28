from kafka import KafkaConsumer
import time
import pg8000
import json

dbname = 'postgres'
user = 'galinov'
password = 'nt7k4'
host = '192.168.77.21'
port = '5432'
i=1
try:
	conn = pg8000.connect(database=dbname, user=user, password=password, host=host, port=port)
	cursor = conn.cursor()

	consumer = KafkaConsumer('finaltask', bootstrap_servers='vm-strmng-s-1.test.local:9092', group_id='my_group')
	
	for message in consumer:
		message_d = json.loads(message.value.decode('utf-8'))
		subject = message_d['subject']
		org_type = message_d['org_type']
		speed = message_d['speed']
		obj_code = message_d['obj_code']
		date = message_d['date']
		vol_down = message_d['vol_down']
		vol_up = message_d['vol_up']
		insert_sql = f"insert into fw_kafka_buffer (subject,org_type,speed,obj_code,date,vol_down,vol_up) VALUES (%s,%s,%s,%s,%s,%s,%s)"
		cursor.execute(insert_sql,[subject,org_type,speed,obj_code,date,vol_down,vol_up])
		if i%1000==0:
			conn.commit()
		i+=1
finally:
	consumer.close()
	conn.close()
