from faker import Faker 	 
from random import randint	 
import json
import random 
from datetime import datetime as dt

fake = Faker() 
date = dt.strftime(dt.now(),'%Y-%m-%d')
def input_data(x): 
 
	cart_data = []
	for i in range(1, x): 
		temp_cart_dict = {}
		temp_cart_dict['id']= i
		temp_cart_dict['userId']= fake.random_int(1, 100000) 
		temp_cart_dict['date']= date
		temp_cart_dict['products']= [{"productId": fake.random_int(1, 20), "quantity": fake.random_int(1, 10)} for _ in range(random.randint(1, 5))]
		temp_cart_dict['__v']= 0
		print(temp_cart_dict)
		cart_data.append(temp_cart_dict)
	with open('s3_data/cart/cart_daily_data_'+date.replace('-','')+'.json', 'w') as fp: 
		json.dump(cart_data, fp) 
	

def main(): 
	number_of_cart = 100000
	input_data(number_of_cart) 
main() 

