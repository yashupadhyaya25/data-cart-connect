from faker import Faker 	 
from random import randint	 
import json
from datetime import datetime as dt

fake = Faker() 
date = dt.strftime(dt.now(),'%Y-%m-%d')

def input_data(x): 

	# dictionary 
	user_data = []
	for i in range(1, x): 
		temp_user_dict = {}
		temp_user_dict['id']= i
		temp_user_dict['email']= fake.email() 
		temp_user_dict['username']= fake.user_name() 
		temp_user_dict['password']= fake.password()
		temp_user_dict['name']= {'firstname' : fake.first_name(),'lastname' : fake.last_name()}
		temp_user_dict['phone']= fake.unique.numerify('#-###-###-####')
		temp_user_dict['__v']= 0
		temp_user_dict['address'] = {'geolocation' : {'lat' : str(fake.latitude()),'long' :str(fake.longitude())},
							 		'city' : fake.city(),
									'street' : fake.address().split('\n')[0],
									'number' : fake.numerify('####'),
									'zipcode' : fake.zipcode()
							 }
		user_data.append(temp_user_dict)
		print(temp_user_dict)
	with open('s3_data/user/user_daily_data_'+date.replace('-','')+'.json', 'w') as fp:  
		json.dump(user_data, fp) 
	

def main(): 
	number_of_user = 100000
	input_data(number_of_user) 
main()