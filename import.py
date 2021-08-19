#!/usr/bin/env python3

import pymysql
import configparser
import pandas
import math
import os
import re
import sys
import uuid
from datetime import date
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from time import time

config = configparser.ConfigParser()
config.read('import.ini')

print("Initailizing db...")
if 'socket' in config['mysql']:
	db = pymysql.connect(unix_socket=config['mysql']['socket'],
						 user=config['mysql']['user'],
						 password=config['mysql']['password'],
						 db=config['mysql']['schema'],
						 charset='utf8',
						 cursorclass=pymysql.cursors.DictCursor)
else:
	db = pymysql.connect(host=config['mysql']['host'],
						 user=config['mysql']['user'],
						 password=config['mysql']['password'],
						 db=config['mysql']['schema'],
						 charset='utf8',
						 cursorclass=pymysql.cursors.DictCursor)
cursor = db.cursor()

data_directory = config['data']['directory'] + '/' + config['data']['version'] + '/'

company = config['wd']['company_id']
line = config['wd']['line_id']
user = config['wd']['user_id']
default_warehouse_code = config['default']['warehouse_code']
now = date.today().isoformat()
unknown_code_ids = { }
get_ids = { }
vendor_check_to = { }
selling_unit_ids = { }
mill_unit_ids = { }

def insert_object(table, object, catch=False):
	insert_object = {}
	for key,value in object.items():
		if isset(value) and isset(key):
			value = str(value).strip()
			if len(value) > 0:
				insert_object[key] = value

	if len(insert_object) == 0:
		return

	query = f"insert into `{table}` (`{'`,`'.join(insert_object.keys())}`) values ({','.join(['%s'] * len(insert_object.values()))})"
	try:
		cursor.execute(query, list(x for x in insert_object.values()))
	except pymysql.err.IntegrityError as e:
		if not catch:
			raise e
	return cursor.lastrowid

def update_set(key):
	return f"`{key}`=%s"

def update_object(table, where_column, where_value, object):
	update_object = {}
	for key,value in object.items():
		if isset(value) and isset(key):
			update_object[key] = value

	if len(update_object) == 0:
		return

	query = f"update `{table}` set {','.join(update_set(key) for key in update_object.keys())} where `company_id` = %s and `{where_column}` = %s"
	cursor.execute(query, list(str(x) for x in update_object.values()) + [company, where_value])

def get_reader(path, header=0, quoting=3):
	reader = pandas.read_csv(path, header=header, delimiter='	', encoding='latin1', low_memory=False, quoting=quoting)
	reader.fillna(value='')
	return reader

def get_iterator(path, header=0, quoting=3):
	return get_reader(path, header, quoting).iterrows()

def isset(check):
	try:
		return not math.isnan(check)
	except:
		return check is not None and check != ''

def isempty(check):
	return not isset(check)

def combine(data, fields, separator=''):
	combined = ''
	for field in fields:
		if field in data and isset(data[field]):
			combined = combined + data[field] + separator
	return combined

def get_state_id(country, code):
	if not isset(code):
		return None

	cursor.execute("select `id` from `state` where `country_id` = %s and (`name` = %s or `code` = %s)", (country, code, code))
	state = cursor.fetchone()
	if state is not None:
		return state['id']
	else:
		return None

def get_country_id(code):
	if not isset(code):
		return None

	cursor.execute("select `id` from `country` where `name` = %s or `code` = %s", (code, code))
	country = cursor.fetchone()
	if country is not None:
		return country['id']
	else:
		return None

def get_id(table, where_column, where_value, company_specific=True):
	key = table + where_column + str(where_value)
	if key not in get_ids:
		result = get_column(table, 'id', where_column, where_value, company_specific)
		if result is not None:
			get_ids[key] = result
		else:
			return None


	return get_ids[key]

def get_column(table, column, where_column, where_value, company_specific=True):
	obj = get_object(table, where_column, where_value, company_specific)
	if obj is None:
		return None
	else:
		return obj[column]

def get_object(table, where_column, where_value, company_specific=True):
	if isempty(where_value):
		return None

	if company_specific:
		cursor.execute(f"select * from `{table}` where `company_id` = %s and `{where_column}` = %s", (company, where_value))
	else:
		cursor.execute(f"select * from `{table}` where `{where_column}` = %s", where_value)
	row = cursor.fetchone()
	if row is None:
		return None
	else:
		return row

def get_selling_unit_id(item_id):
	if item_id not in selling_unit_ids:
		cursor.execute('select `selling_unit_id` from `style` inner join `item` on `style`.`id` = `item`.`style_id` where `item`.`id` = %s', item_id)
		selling_unit_ids[item_id] = cursor.fetchone()['selling_unit_id']

	return selling_unit_ids[item_id]

def get_mill_unit_id(item_id):
	if item_id not in mill_unit_ids:
		cursor.execute('select `mill_unit_id` from `style` inner join `item` on `style`.`id` = `item`.`style_id` where `item`.`id` = %s', item_id)
		mill_unit_ids[item_id] = cursor.fetchone()['mill_unit_id']

	return mill_unit_ids[item_id]

def get_code_id(type_id, abbreviation, allow_unknown=False):
	if not isset(abbreviation):
		if allow_unknown:
			return get_unknown_code(type_id)
		else:
			return None

	key = 'code' + str(type_id) + str(abbreviation)
	if key not in get_ids:
		cursor.execute("select `id` from `code` where `company_id` = %s and `type_id` = %s and (`abbreviation` = %s or `name` = %s)", (company, type_id, str(abbreviation), str(abbreviation)))
		code = cursor.fetchone()

		if code is not None:
			get_ids[key] = code['id']
		elif allow_unknown:
			get_ids[key] = get_unknown_code(type_id)
		else:
			return None

	return get_ids[key]

def get_unknown_code(type_id):
	if type_id not in unknown_code_ids:
		unknown_code_ids[type_id] = create_code_id('UNK', type_id, 'Unknown')
	return unknown_code_ids[type_id]

def create_code_id(abbreviation, type_id, name=None):
	if not isset(abbreviation):
		return None

	if name is None:
		name = abbreviation
	existing = get_code_id(type_id, abbreviation)
	if existing is None:
		insert_object('code', {
			'company_id': company,
			'type_id': type_id,
			'name': name,
			'abbreviation': abbreviation
		})
		db.commit()
		return cursor.lastrowid
	else:
		return existing

def get_custom_field_id(resource, name, field_type=1, default=None):
	cursor.execute("select `id` from `custom_field` where `company_id` = %s and `resource_class` = %s and `name` = %s", (company, resource, name))
	existing = cursor.fetchone()
	if existing is not None:
		return existing['id']
	else:
		insert_object('custom_field', {
			'company_id': company,
			'created_at': now,
			'default_value': default,
			'name': name,
			'resource_class': resource,
			'type': field_type,
			'updated_at': now,
		})
		db.commit()
		return cursor.lastrowid

def next_sequential_number(column):
	value = get_column('sequential_number', column, 'company_id', company)
	cursor.execute(f"update `sequential_number` set `{column}` = `{column}` + 1 where `company_id` = %s", company)
	db.commit()
	return value

def insert_address(source_id, source_type, street, city, country_id, name='Primary', street2=None, state_id=None, postal_code=None, attention=None, company_name=None):
	address = {
		'addressable_id': source_id,
		'addressable_type': source_type,
		'city': city,
		'company_name': company_name,
		'country_id': country_id,
		'created_at': now,
		'name': name,
		'primary': '1',
		'street': street,
		'updated_at': now,
	}

	if street2 is not None:
		address['street2'] = street2

	if state_id is not None:
		address['state_id'] = state_id

	if postal_code is not None:
		address['postal_code'] = postal_code

	if attention is not None:
		address['attention'] = attention

	try:
		return insert_object('address', address)
	except Exception as e:
		print(f"Unable to import address ({source_id}/{e})")
		return None

def insert_phone(source_id, source_type, country_id, phone_number, primary=True):
	phone_number = re.sub(r'[^\d]', '', phone_number)

	phone = {
		'country_id': country_id,
		'created_at': now,
		'phone_number': phone_number,
		'phoneable_id': source_id,
		'phoneable_type': source_type,
		'updated_at': now,
	}

	if primary:
		phone['primary'] = '1'
		phone['name'] = 'Primary'
	else:
		phone['primary'] = '0'
		phone['name'] = phone_number

	try:
		return insert_object('phone', phone)
	except:
		return None

def add_transaction_hold(transaction_id, hold_code):
	hold_id = create_code_id(hold_code, 10)
	if hold_id is not None:
		try:
			insert_object('transaction_hold', { 'transaction_id': transaction_id, 'hold_id': hold_id, 'created_at': now, 'updated_at': now })
		except pymysql.err.IntegrityError:
			print('Duplicate transaction hold')

def import_code(data):
	code = {
		'company_id': company,
		'name': data['DESCRIPTION'],
		'abbreviation': data['CODE'],
		'created_at': now,
		'updated_at': now,
	}

	if data['TYPE'] == 'P':
		code['type_id'] = 1
	elif data['TYPE'] == 'C':
		code['type_id'] = 21
	elif data['TYPE'] == 'R':
		code['type_id'] = 23
	elif data['TYPE'] == 'E':
		code['type_id'] = 26
	elif data['TYPE'] == 'T' or data['TYPE'] == 'V':
		if data['PROFORMA^FLAG'] == 'Y':
			code['at_proforma'] = 1
		else:
			code['at_terms_days'] = data['EXTRA^DAYS 1']
		code['type_id'] = 13
	elif data['TYPE'] == 'I':
		code['type_id'] = 16
	elif data['TYPE'] == 'J':
		code['type_id'] = 22
	elif data['TYPE'] == 'L':
		code['type_id'] = 2
	elif data['TYPE'] == 'G':
		code['type_id'] = 32
	elif data['TYPE'] == 'D':
		code['type_id'] = 8
	elif data['TYPE'] == 'S':
		if data['TYPE'] == 'MS':
			code['st_inventoried'] = '1';
		else:
			code['st_inventoried'] = '0';
		code['type_id'] = 31
	else:
		return

	try:
		insert_object('code', code)
	except pymysql.err.IntegrityError:
		print(f"Duplicate code ({data['TYPE']}/{data['CODE']})")

def import_warehouse(data):
	return insert_object('warehouse', {
		'code': data['CODE'],
		'company_id': company,
		'country_id': country_us_id,
		'created_at': now,
		'currency_id': currency_usd_id,
		'name': data['DESCRIPTION'],
		'shipping_zone': 'A',
		'updated_at': now,
	})

def import_rep(data):
	if isempty(data['REP NAME']):
		data['REP NAME'] = data['REP^ID']

	if isempty(data['REP NAME']):
		print("Skipping rep without name or rep id")
		return None

	try:
		date_established = parse(data['DATE^EST'])
	except:
		date_established = date.today()

	rep = {
		'company_id': company,
		'currency_id': currency_usd_id,
		'created_at': date_established.isoformat(),
		'fax': data['FAX'],
		'name': data['REP NAME'],
		'rep_code': data['REP^ID'],
		'standard_commission_percent': data['COMM^PCT'] / 100,
		'start_date': date_established.isoformat(),
		'territory_id': create_code_id(data['TERR'], 9),
		'updated_at': date_established.isoformat(),
	}

	if isset(data['END^DATE']):
		try:
			rep['end_date'] = parse(data['END^DATE']).isoformat()
		except:
			print(f"Invalid end date ({data['END^DATE']})")

	if isset(data['REP NAME']) and pattern_deleted_rep.match(data['REP NAME']):
		rep['deleted_at'] = now

	cursor.execute("select * from `rep` where `company_id` = %s and `name` = %s", (company, str(data['REP NAME'])))
	if cursor.fetchone() is not None:
		rep['name'] = f"{data['REP NAME']} ({data['REP^ID']})"

	if str(data['COUNTRY'])[:2] == 'US':
		data['COUNTRY'] = 'US'
	elif str(data['COUNTRY'])[:2] == 'CA':
		data['COUNTRY'] = 'CA'
	else:
		data['COUNTRY'] = 'US'

	rep['country_id'] = get_country_id(data['COUNTRY'])

	rep_id = insert_object('rep', rep)

	if isset(data['ADDRESS ONE']):
		insert_address(source_id=rep_id,
					   source_type='Rep',
					   street=data['ADDRESS ONE'],
					   street2=data['ADDRESS TWO'],
					   city=data['CITY'],
					   state_id=get_state_id(rep['country_id'], data['ST']),
					   postal_code=data['ZIP CODE'],
					   country_id=rep['country_id'],
					   attention=data['ATTENTION'],
					   company_name=rep['name'])

	if isset(data['PHONE ONE']):
		insert_phone(source_id=rep_id,
					 source_type='Rep',
					 country_id=rep['country_id'],
					 phone_number=data['PHONE ONE'])

def import_rep_employee(data):
	rep = get_object('rep', 'rep_code', data['REP #'])
	if rep is None:
		print(f"Skipping employee with missing rep {data['REP ID #']}")
		return

	if isset(data['EMAIL ADDRESS']):
		at_position = data['EMAIL ADDRESS'].find('@')
		employee = {
			'company_id': company,
			'country_id': rep['country_id'],
			'created_at': now,
			'customer_service': 1,
			'employer_id': rep['id'],
			'employer_type': 'Rep',
			'updated_at': now,
		}
		if at_position > 0:
			employee['name'] = data['EMAIL ADDRESS'][:at_position]
			if isset(data['EMPLOYEE NAME']):
				employee['name'] = data['EMPLOYEE NAME']
			employee_id = insert_object('employee', employee)
			email = {
				'created_at': now,
				'email_address': data['EMAIL ADDRESS'],
				'emailable_id': employee_id,
				'emailable_type': 'Employee',
				'name': 'Primary',
				'primary': 1,
				'updated_at': now,
			}
			insert_object('email', email)
		else:
			print(f"Skipping rep employee with invalid email address ({data['EMAIL ADDRESS']})")

def import_vendor(data):
	if isempty(data['VENDOR NAME']):
		data['VENDOR NAME'] = data['VENDOR^ID']

	if isempty(data['VENDOR NAME']):
		print("Skipping vendor without name")
		return None

	try:
		date_established = parse(data['DATE^EST'])
	except:
		date_established = date.today()

	vendor = {
		# 'fax': data['FAX'],
		'account_number': data['OUR^CUST ID'],
		'company_id': company,
		'created_at': date_established.isoformat(),
		'date_established': date_established.isoformat(),
		'delivery_days': data['SHIP^DAYS'],
		'legacy_vendor_code': data['VENDOR^ID'],
		'name': data['VENDOR NAME'],
		'notes': data['COMMENT'],
		'ship_via': data['SHIP VIA'],
		'terms_id': get_code_id(13, data['TERMS^CODE'], True),
		'updated_at': date_established.isoformat(),
		'cut_yardage': '1' if data['DROP^SHIP^FLAG'] == 'Y' else '0',
	}

	cursor.execute("select * from `vendor` where `company_id` = %s and `name` = %s", (company, str(data['VENDOR NAME'])))
	if cursor.fetchone() is not None:
		vendor['name'] = f"{data['VENDOR NAME']} ({data['VENDOR^ID']})"

	cursor.execute("select * from `vendor` where `company_id` = %s and `name` = %s", (company, str(data['VENDOR NAME'])))
	if cursor.fetchone() is not None:
		vendor['name'] = f"{data['VENDOR NAME']} ({data['VENDOR^ID']})"

	if isset(data['COUNTRY']) and re.sub(r'[^\w]', '', data['COUNTRY']) == 'UK':
		data['COUNTRY'] = 'GB'

	vendor['country_id'] = get_country_id(data['COUNTRY'])

	if isempty(vendor['country_id']):
		vendor['country_id'] = country_us_id

	if data['CURRENCY^CODE'] == "EU" or data['CURRENCY^CODE'] == "DM" or data['CURRENCY^CODE'] == "DK" or data['CURRENCY^CODE'] == "FF":
		data['CURRENCY^CODE'] = 'EUR'

	elif data['CURRENCY^CODE'] == "PO":
		data['CURRENCY^CODE'] = "GBP";

	elif data['CURRENCY^CODE'] == "YE":
		data['CURRENCY^CODE'] = "JPY";

	elif data['CURRENCY^CODE'] == "CD":
		data['CURRENCY^CODE'] = 'CAD'

	else:
		data['CURRENCY^CODE'] = 'USD'

	vendor['currency_id'] = get_id('currency', 'code', data['CURRENCY^CODE'], False)

	if vendor['country_id'] != country_us_id:
		vendor['foreign'] = '1'

	if data['VENDOR^PHONE^TYP 1'] == 'F':
		vendor['fax'] = data['VENDOR^PHONE 1']
	elif data['VENDOR^PHONE^TYP 2'] == 'F':
		vendor['fax'] = data['VENDOR^PHONE 2']

	vendor_id = insert_object('vendor', vendor)

	if isset(data['SEND^CHK^TO']):
		vendor_check_to[vendor_id] = data['SEND^CHK^TO']

	if isset(data['VENDOR ADDRESS 1']):
		insert_address(source_id=vendor_id,
					   source_type='Vendor',
					   street=data['VENDOR ADDRESS 1'],
					   street2=data['VENDOR ADDRESS 2'],
					   city=data['VENDOR CITY'],
					   state_id=get_state_id(vendor['country_id'], data['VENDOR^STATE']),
					   postal_code=data['VENDOR^ZIP CODE'],
					   country_id=vendor['country_id'],
					   attention=data['VENDOR ATTENTION'],
					   company_name=vendor['name'])

	if data['VENDOR^PHONE^TYP 1'] == 'V' or data['VENDOR^PHONE^TYP 1'] == 'P':
		insert_phone(source_id=vendor_id,
					 source_type='Vendor',
					 country_id=vendor['country_id'],
					 phone_number=data['VENDOR^PHONE 1'])

	if data['VENDOR^PHONE^TYP 2'] == 'V' or data['VENDOR^PHONE^TYP 2'] == 'P':
		insert_phone(source_id=vendor_id,
					 source_type='Vendor',
					 country_id=vendor['country_id'],
					 phone_number=data['VENDOR^PHONE 2'],
					 primary=False)

def import_vendor_employee(data):
	vendor = get_object('vendor', 'legacy_vendor_code', data['CUSTOMER #'])
	if vendor is None:
		print(f"Skipping employee with missing vendor {data['CUSTOMER #']}")
		return

	if isset(data['EMAIL ADDRESS']):
		at_position = data['EMAIL ADDRESS'].find('@')
		employee = {
			'company_id': company,
			'country_id': vendor['country_id'],
			'created_at': now,
			'customer_service': 1,
			'employer_id': vendor['id'],
			'employer_type': 'Vendor',
			'updated_at': now,
		}
		if at_position > 0:
			employee['name'] = data['EMAIL ADDRESS'][:at_position]
			if isset(data['EMPLOYEE NAME']):
				employee['name'] = data['EMPLOYEE NAME']
			employee_id = insert_object('employee', employee)
			email = {
				'created_at': now,
				'email_address': data['EMAIL ADDRESS'],
				'emailable_id': employee_id,
				'emailable_type': 'Employee',
				'name': 'Primary',
				'primary': 1,
				'updated_at': now,
			}
			insert_object('email', email)
		else:
			print(f"Skipping vendor employee with invalid email address ({data['EMAIL ADDRESS']})")

def import_customer(data):
	if isempty(data['CUSTOMER NAME']):
		data['CUSTOMER NAME'] = data['CUST #']

	try:
		date_established = parse(data['DATE EST'])
	except:
		date_established = date.today()

	try:
		rep = get_object('rep', 'rep_code', data['REP^ID'])
		if rep is None:
			rep = unknown_rep
	except:
		rep = unknown_rep

	# TODO: Check currency
	currency_id = currency_usd_id

	if data['D^F'] == 'L':
		discount_method = 'L'
	else:
		discount_method = 'B'

	customer = {
		'class_id': create_code_id(data['CUST CLASS'], 24),
		'company_id': company,
		'country_id': country_us_id,
		'created_at': date_established.isoformat(),
		'credit_alert_id': create_code_id(data['CREDIT^ALERT'], 12),
		'credit_limit': data['LIMIT'],
		'currency_id': currency_id,
		'customer_number': data['CUST #'],
		'default_carrier_id': carrier_def_id,
		'default_shipping_service_id': shipping_service_def_id,
		'discount_method': discount_method,
		'discount_percent': data['DISC^PCT'] / 100,
		'fax': data['FAX'],
		'federal_tax_exempt': '0',
		'legacy_customer_number': data['CUST #'],
		'name': data['CUSTOMER NAME'],
		'rep_id': rep['id'],
		'state_tax_exempt': '0',
		'terms_id': get_code_id(13, data['TERMS^CODE'], True),
		'type_id': '1',
		'updated_at': now,
	}

	if isset(data['ADDRESS ONE']) and pattern_deleted_customer.match(data['ADDRESS ONE']):
		customer['deleted_at'] = now

	if data['CUST TYPE'] == 'P' or data['CUST TYPE'] == 'B':
		customer['purchasing_agent'] = '1'
		customer['type_id'] = '3'

	if data['CUST TYPE'] == 'S' or data['CUST TYPE'] == 'B':
		customer['specifier'] = '1'
		customer['type_id'] = '3'

	customer_id = insert_object('customer', customer)

	state_id = get_state_id(customer['country_id'], data['ST'])

	if customer['country_id'] == country_us_id and not isempty(data['RESALE NUMBER']) and not isempty(state_id):
		resale_data = {
			'created_at': now,
			'customer_id': customer_id,
			'resale_number': data['RESALE NUMBER'],
			'state_id': state_id,
			'updated_at': now,
		}
		if not isempty(data['RESALE^NUMBER^DATE']):
			try:
				resale_data['expiration_date'] = parse(data['RESALE^NUMBER^DATE']).isoformat()
			except:
				print(f"Invalid resale expiration date ({data['RESALE^NUMBER^DATE']})")

		insert_object('resale_certificate', resale_data)

	if isset(data['ADDRESS ONE']) and isset(data['CITY']):
		insert_address(source_id=customer_id,
					   source_type='Customer',
					   street=data['ADDRESS ONE'],
					   street2=data['ADDRESS TWO'],
					   city=data['CITY'],
					   state_id=state_id,
					   postal_code=data['ZIP CODE'],
					   country_id=customer['country_id'],
					   attention=data['ATTENTION'],
					   company_name=customer['name'])

	if isset(data['PHONE 1']):
		insert_phone(source_id=customer_id,
					 source_type='Customer',
					 country_id=customer['country_id'],
					 phone_number=data['PHONE 1'])

	if isset(data['PHONE 2']):
		insert_phone(source_id=customer_id,
					 source_type='Customer',
					 country_id=customer['country_id'],
					 phone_number=data['PHONE 2'],
					 primary=False)

def import_customer_note(data, note_type=None):
	customer_id = get_id('customer', 'customer_number', data[0])

	if customer_id is None:
		print(f"Unable to import notes for missing customer #{data[0]}")
		return

	note = {
		'company_id': company,
		'content': '',
		'created_at': now,
		'noteable_id': customer_id,
		'noteable_type': 'Customer',
		'type': note_type,
		'updated_at': now,
		'user_id': user,
	}

	for column in data[1:]:
		if isset(column) and len(column.strip()) > 0:
			note['content'] = note['content'] + column.strip() + '\n'

	if len(note['content'].strip()) > 0:
		insert_object('note', note)

def import_customer_employee(data):
	customer = get_object('customer', 'customer_number', data['CUSTOMER #'])
	if customer is None:
		print(f"Skipping employee with missing customer #{data['CUSTOMER #']}")
		return

	employee = {
		'company_id': company,
		'country_id': customer['country_id'],
		'created_at': now,
		'employer_id': customer['id'],
		'employer_type': 'Customer',
		'name': data['EMPLOYEE NAME'],
		'notes': data['COMMENT1'],
		'updated_at': now,
		'title_id': create_code_id(data['JOB CODE'], 22)
	}

	if isempty(employee['name']):
		if isset(data['EMAIL ADDRESS']):
			at_position = data['EMAIL ADDRESS'].find('@')
			if at_position > 0:
				employee['name'] = data['EMAIL ADDRESS'][:at_position]
			else:
				employee['name'] = data['EMAIL ADDRESS']
		else:
			print(f"Skipping employee without name or email address ({data['CUSTOMER #']})")
			return

	employee['name'] = employee['name'][:50]

	employee_id = insert_object('employee', employee)

	if isset(data['EMAIL ADDRESS']):
		email = {
			'created_at': now,
			'email_address': data['EMAIL ADDRESS'],
			'emailable_id': employee_id,
			'emailable_type': 'Employee',
			'updated_at': now,
		}

		if data['SEQ #'] == '001':
			email['primary'] = 1
			email['name'] = 'Primary'
		else:
			email['name'] = data['EMAIL ADDRESS'][:50]

		insert_object('email', email)

	if isset(data['EMPLOYEE PHONE']):
		insert_phone(employee_id, 'Employee', customer['country_id'], data['EMPLOYEE PHONE'])

def import_customer_ship_to_address(data):
	customer = get_object('customer', 'customer_number', data['CUST #'])
	if customer is None:
		print(f"Skipping ship to address for missing customer #{data['CUST #']}")
		return

	if isempty(data['ADDRESS ONE']):
		# print(f"Skipping ship to address without street for customer #{data['CUST #']}")
		return

	ship_to = {
		'country_id': customer['country_id'],
		'created_at': now,
		'customer_id': customer['id'],
		'name': data['NAME'],
		'updated_at': now,
	}

	if isempty(ship_to['name']):
		ship_to['name'] = 'Unknown'

	ship_to_id = insert_object('ship_to', ship_to)

	address_id = insert_address(source_id=ship_to_id,
								source_type='ShipTo',
								street=data['ADDRESS ONE'],
								street2=data['ADDRESS TWO'],
								city=data['CITY'],
								state_id=get_state_id(customer['country_id'], data['ST']),
								postal_code=data['ZIP CODE'],
								country_id=customer['country_id'],
								attention=data['ATTENTION'],
								company_name=ship_to['name'])
	if address_id is None:
		cursor.execute("delete from `ship_to` where `id` = %s", ship_to_id)

def import_customer_resale(data):
	customer_id = get_id('customer', 'customer_number', data['CUSTOMER^NUMBER'])
	if customer_id is None:
		print(f"Skipping resale certificate for missing customer #{data['CUSTOMER^NUMBER']}")
		return

	record = {
		'created_at': now,
		'customer_id': customer_id,
		'state_id': get_state_id(country_us_id, data['RESALE^STATE1']),
		'updated_at': now,
	}

	if isset(data['EXPIRATION^DATE 1']):
		record['expiration_date'] = parse(data['EXPIRATION^DATE 1']).strftime('%Y-%m-%d')

	if isset(data['RESALE^NUMBER1']):
		record['resale_number'] = data['RESALE^NUMBER1']
	else:
		record['resale_number'] = 'On File'

	if isset(record['state_id']):
		insert_object('resale_certificate', record)

def import_style(data):
	try:
		intro_date = parse(data['INTRODUCTION^DATE'])
		if intro_date < cutoff_date:
			intro_date = cutoff_date
	except:
		intro_date = date.today()

	style = {
		'broker_cost': data['ESTIMATED^FRT'],
		'comment': combine(data, ('COMMENT 1', 'COMMENT 2', 'COMMENT 3'), "\n"),
		'company_id': company,
		'content': combine(data, ('CONTENT 1', 'CONTENT 2'), "\n"),
		'created_at': intro_date.strftime('%Y-%m-%d'),
		'date_introduced': intro_date.strftime('%Y-%m-%d'),
		'default_warehouse_id': warehouse_def_id,
		'duty_percent': data['DUTY^PERCENT'] / 100,
		'finish': data['FINISH'],
		'label_message': combine(data, ('LABEL DESCRIPTION 1', 'LABEL DESCRIPTION 2'), "\n"),
		'lead_time': data['LEAD^TIME'] * 7,
		'legacy_style_number': data['PATTERN^NUMBER'],
		'line_id': line,
		'minimum_order_quantity': data['MILLS^MINIMUM^ORDER'],
		'misc_cost': data['MISC^COST'],
		'name': data['PATTERN NAME'],
		'origin_country': data['COUNTRY^OF ORIGIN'],
		'package_code_id': code_standard_package_id,
		'pattern_flag': data['PRODUCT^FLAG'],
		'product_category_code_id': get_code_id(1, data['PRODUCT^LINE'], True),
		'repeat': data['REPEAT'],
		'royalty_percent': data['ROYALTY^RATE'] / 100,
		'tests': combine(data, ('TEST 1', 'TEST 2'), "\n"),
		'updated_at': now,
		'weight': int(data['WEIGHT^POUNDS']) * 16 + int(data['WEIGHT^OUNCES']),
		'width': data['WIDTH'],
	}

	style['standard_quantity'] = max(style['minimum_order_quantity'], 1)
	style['shipping_weight'] = style['weight']

	if isempty(style['name']):
		style['name'] = data['PATTERN^NUMBER']

	if isset(data['UNIT^OF^MEASURE']):
		if data['UNIT^OF^MEASURE'][0] == 'Y':
			style['selling_unit_id'] = code_yard_id
		elif data['UNIT^OF^MEASURE'][0] == 'M':
			style['selling_unit_id'] = code_meter_id
		elif data['UNIT^OF^MEASURE'][0] == 'R':
			style['selling_unit_id'] = code_roll_id
		elif data['UNIT^OF^MEASURE'].strip() == 'EACH':
			style['selling_unit_id'] = code_each_id
		elif pattern_valid_unit.match(data['UNIT^OF^MEASURE']):
			# In the future, this should be modified to automatically create the unit of measure
			pass

	if 'selling_unit_id' not in style:
		style['selling_unit_id'] = code_unknown_unit_id

	if isset(data['MILLS^UNIT OF^MEAS']):
		if data['MILLS^UNIT OF^MEAS'][0] == 'Y':
			style['mill_unit_id'] = code_yard_id
		elif data['MILLS^UNIT OF^MEAS'][0] == 'M':
			style['mill_unit_id'] = code_meter_id
		elif data['MILLS^UNIT OF^MEAS'][0] == 'R':
			style['mill_unit_id'] = code_roll_id
		elif data['MILLS^UNIT OF^MEAS'].strip() == 'EACH':
			style['mill_unit_id'] = code_each_id
		elif pattern_valid_unit.match(data['MILLS^UNIT OF^MEAS']):
			# In the future, this should be modified to automatically create the unit of measure
			pass

	if 'mill_unit_id' not in style:
		style['mill_unit_id'] = code_unknown_unit_id

	if isset(data['VENDOR^CODE']):
		style['vendor_id'] = get_id('vendor', 'legacy_vendor_code', data['VENDOR^CODE'])

	if 'vendor_id' not in style or isempty(style['vendor_id']):
		style['vendor_id'] = unknown_vendor['id']

	cursor.execute("select * from `style` where `company_id` = %s and `name` = %s", (company, str(data['PATTERN NAME'])))
	if cursor.fetchone() is not None:
		style['name'] = f"{data['PATTERN NAME']} ({data['PATTERN^NUMBER']})"

	style_id = insert_object('style', style)

	if data['PRICE 1'] > 0:
		insert_object('item_price', {
			'company_id': company,
			'line_id': line,
			'currency_id': currency_usd_id,
			'unit_id': style['selling_unit_id'],
			'wholesale_price': data['PRICE 1'],
			'priceable_id': style_id,
			'priceable_type': 'Style',
			'primary': 1,
			'created_at': now,
			'updated_at': now,
		})

	if data['MILLS^COST 1'] > 0:
		insert_object('style_cost', {
			'style_id': style_id,
			'effective_date': now,
			'minimum_quantity': '0',
			'cost': data['MILLS^COST 1'],
			'created_at': now,
			'updated_at': now,
		})

	if data['MILLS^COST 2'] > 0:
		try:
			insert_object('style_cost', {
				'style_id': style_id,
				'effective_date': now,
				'minimum_quantity': data['COST^BREAK^POINT 2'],
				'cost': data['MILLS^COST 2'],
				'created_at': now,
				'updated_at': now,
			})
		except pymysql.err.IntegrityError:
			print(f"Not importing duplicate cost ({style['name']} - {data['COST^BREAK^POINT 2']})")
		except pymysql.err.DataError:
			print(f"Not importing invalid cost ({data['MILLS^COST 2']})")

	if data['MILLS^COST 3'] > 0:
		try:
			insert_object('style_cost', {
				'style_id': style_id,
				'effective_date': now,
				'minimum_quantity': data['COST^BREAK^POINT 3'],
				'cost': data['MILLS^COST 3'],
				'created_at': now,
				'updated_at': now,
			})
		except pymysql.err.IntegrityError:
			print(f"Not importing duplicate cost ({style['name']} - {data['COST^BREAK^POINT 3']})")
		except pymysql.err.DataError:
			print(f"Not importing invalid cost ({data['MILLS^COST 3']})")

def import_harmonized(data):
	harmonized = {
		'synthetic_artificial': combine(data, ('SYNTHETIC^ARTIFICIA 1', 'SYNTHETIC^ARTIFICIA 2')),
		'how_woven': data['HOW WOVEN'],
		'fabric_type': data['TYPE OF FABRIC'],
		'grams_per_square_meter': data['GRAMS/^SQ^METER'],
		'threads_per_square_cm': data['THREADS/^SQ^CM'],
		'average_yarn_number': data['AVG^YARN^NUMBER'],
		'napped': data['NAPPED^OR^NOT^NAPPD'],
		'combed_carded': data['COMBED^OR^CARDED'],
		'technical_comment': combine(data, ('TECHNICAL COMMENT 1', 'TECHNICAL COMMENT 2')),
		'purchase_comment': combine(data, ('PURCHASE COMMENT 1', 'PURCHASE COMMENT 2')),
	}

	if isset(data['YARNS^DIFFER^COLOR']) and data['YARNS^DIFFER^COLOR'] != 'N':
		harmonized['yarns_different_color'] = '1'

	if isset(data['H.S.CODE']):
		create_harmonized_code(data['PATTERN^NUMBER'], data['H.S.CODE'], country_us_id)

	try:
		update_object('style', 'legacy_style_number', data['PATTERN^NUMBER'], harmonized)
	except Exception as e:
		print(f"Unable to set harmonized code data ({data['PATTERN^NUMBER']}/{str(e)})")

def create_harmonized_code(legacy_style_number, harmonized_code, country_id):
	style_id = get_id('style', 'legacy_style_number', legacy_style_number)
	if style_id is not None:
		insert_object('style_harmonized', {
			'country_id': country_id,
			'harmonized_code': harmonized_code,
			'style_id': style_id,
		})

def get_discontinue_date(value):
	match = pattern_discontinue_code.match(value)
	if match:
		comet_date = match.group(3)
		if comet_date.count('/') == 1:
			comet_date_arr = comet_date.split('/', 2)
			comet_date = comet_date_arr[0] + '/01/' + comet_date_arr[1]

		try:
			return parse(comet_date)
		except:
			return date.today()
	else:
		return date.today()

def get_discontinue_code_id(value):
	match = pattern_discontinue_code.match(value)
	if match:
		custom_code = create_code_id(match.group(2), 7)
		if custom_code is not None:
			return custom_code
	return code_discontinued_id

def import_item(data):
	item = {
		'company_id': company,
		'line_id': line,
		'item_number': data['ITEM^NUMBER'],
		'style_id': get_id('style', 'legacy_style_number', data['PATTERN^NUMBER']),
		'color_name': data['COLOR^NAME'],
		'mill_description': data['MILL ITEM^NUMBER'],
		'comment': combine(data, ('COLOR COMMENT 1', 'COLOR COMMENT 2')),
		'reorder_point': data['REORDER^POINT'],
		'created_at': now,
		'updated_at': now,
	}

	if isempty(item['style_id']):
		item['style_id'] = unknown_style['id']

	if isset(data['DISCONTINUE^DATE']):
		item['discontinue_code_id'] = get_discontinue_code_id(data['DISCONTINUE^DATE'])
		item['date_discontinued'] = get_discontinue_date(data['DISCONTINUE^DATE']).isoformat()

	if data['CUST^ITEM^FLG'] == 'Y':
		item['custom_item'] = '1'

	if data['COLOR COMMENT 1'] == '***' and 'date_discontinued' not in item:
		item['limited_stock_date'] = now
		item['comment'] = data['COLOR COMMENT 2']

	try:
		item_id = insert_object('item', item)
	except pymysql.err.IntegrityError:
		print(f"Skipping duplicate item #{data['ITEM^NUMBER']}")

def import_service(data):
	service = {
		'company_id': company,
		'item_number': data['SERVICE ID'],
		'description': data['DESCRIPTION'],
		'price': data['PRICE'],
	}

	if isset(data['SELLING UNIT']):
		if data['SELLING UNIT'][0] == 'Y':
			service['unit_id'] = code_yard_id
		elif data['SELLING UNIT'][0] == 'M':
			service['unit_id'] = code_meter_id
		elif data['SELLING UNIT'].strip() == 'EACH':
			service['unit_id'] = code_each_id
	if 'unit_id' not in service:
		service['unit_id'] = code_unknown_unit_id

	insert_object('service', service)

def import_inventory(data):
	try:
		date_received = parse(data['DATE^RCVD^WHSE'])
		if date_received < cutoff_date:
			print(f"Invalid date received ({data['DATE^RCVD^WHSE']}); using today's date")
			date_received = cutoff_date
	except:
		date_received = date.today()

	inventory = {
		'company_id': company,
		'line_id': line,
		'user_id': user,
		'vendor_id': unknown_vendor['id'],
		'item_id': get_id('item', 'item_number', data['ITEM #']),
		'lot': data['LOT #'],
		'piece': data['PCE'],
		'mill_piece': data['MILLS^PIECE ID'],
		'warehouse_id': get_id('warehouse', 'code', data['WHSE^CODE']),
		'warehouse_location': data['WHSE^LOCATION'],
		'comment': data['COMMENT'],
		'quantity': data['ON HAND'],
		'exchange_rate_entry': 1,
		'exchange_rate_receipt': 1,
		'company_exchange_rate_entry': 1,
		'company_exchange_rate_receipt': 1,
		'source_id': 0,
		'source_type': 'Import',
		'created_at': date_received.isoformat(),
		'updated_at': now,
		'accounting_period': date_received.strftime('%Y-%m-01'),
		'material_unit_cost': data['MATERIAL^COST'],
		'duty_unit_cost': data['DUTY^COST'],
		'broker_unit_cost': data['BROKER^COST'],
		'finish_unit_cost': data['FINISH^COST'],
		'misc_unit_cost': data['MISC^COST'],
		'print_tag': 0,
	}

	if inventory['item_id'] is None:
		print(f"Skipping missing item ({data['ITEM #']})")
		return

	if not data['ON HAND'] > 0:
		inventory['active'] = 0

	try:
		insert_object('inventory', inventory)
	except pymysql.err.InternalError as e:
		print(f"Unable to import piece for {data['ITEM #']}/{data['LOT #']}/{data['PCE']}: {e}")

def import_purchase_order(data):
	vendor = get_object('vendor', 'legacy_vendor_code', data['VEND #'])
	if vendor is None:
		vendor = get_object('vendor', 'legacy_vendor_code', 'UNK')

	if isempty(data['TERMS^CODE']):
		terms_code = code_unknown_terms_id
	elif isinstance(data['TERMS^CODE'], (float)):
		terms_code = get_code_id(13, round(data['TERMS^CODE']))
	else:
		terms_code = get_code_id(13, data['TERMS^CODE'])

	purchase_order = {
		'company_id': company,
		'created_at': now,
		'currency_id': vendor['currency_id'],
		'discount_percent': data['DISCOUNT^PERCENT'] / 100,
		'fob': data['FOB'],
		'freight_terms': data['FREIGHT TERMS'],
		'line_id': line,
		'purchase_order_number': re.sub(r'[^\d]', '', data['P.O. #']),
		'ship_to_attention': data['ATTENTION'],
		'ship_to_city': data['CITY'],
		'ship_to_country_id': country_us_id,
		'ship_to_name': data['NAME'],
		'ship_to_state_id': get_state_id(country_us_id, data['ST']),
		'ship_to_street': data['ADDRESS 1'],
		'ship_to_street2': data['ADDRESS 2'],
		'ship_via': data['SHIP VIA'],
		'terms_code_id': terms_code,
		'updated_at': now,
		'user_id': user,
		'vendor_id': vendor['id'],
		'warehouse_id': warehouse_def_id
	}

	insert_object('purchase_order', purchase_order)

def import_purchase_order_item(data):
	purchase_order = get_object('purchase_order', 'purchase_order_number', re.sub(r'[^\d]', '', data['P.O. #']))
	item_id = get_id('item', 'item_number', data['ITEM^#'])

	if purchase_order is None:
		print(f"Skipping PO item for missing PO #{data['P.O. #']}")
		return
	elif item_id is None:
		print(f"Skipping PO item for missing item #{data['ITEM^#']}")
		return

	record = {
		'broker_unit_cost': data['ESTIMATED^FREIGHT'],
		'comment': data['COMMENT'],
		'company_exchange_rate': 1,
		'confirmation_comment': data['CONF #'],
		'created_at': now,
		'item_id': item_id,
		'line_number': data['LINE^#'],
		'material_cost': data['UNIT^COST^DOLLARS'],
		'material_cost_mill': data['P.O^COST'],
		'mill_unit_id': get_mill_unit_id(item_id),
		'misc_unit_cost': data['MSC^COST'],
		'original_quantity': data['QTY ORDERED'],
		'original_quantity_mill': data['P.O.^QTY^ORDERED'],
		'purchase_order_id': purchase_order['id'],
		'remaining_quantity': data['QTY ORDERED'],
		'remaining_quantity_mill': data['P.O.^QTY^ORDERED'],
		'updated_at': now,
	}

	if isset(data['MILL^SHIP^DATE']):
		record['mill_ship_date'] = parse(data['MILL^SHIP^DATE']).isoformat()

	if isset(data['OUR^SHIP^DATE']):
		record['ship_date'] = parse(data['OUR^SHIP^DATE']).isoformat()

	if record['original_quantity'] != 0:
		record['conversion_factor'] = record['original_quantity_mill'] / record['original_quantity']
	else:
		record['conversion_factor'] = 0

	if record['material_cost'] != 0:
		record['exchange_rate'] = record['material_cost_mill'] * record['conversion_factor'] / record['material_cost']
	else:
		record['exchange_rate'] = 0

	insert_object('purchase_order_item', record)

def import_purchase_order_note(data):
	cursor.execute("select `purchase_order_item`.`id` from `purchase_order_item` inner join `purchase_order` on `purchase_order_item`.`purchase_order_id` = `purchase_order`.`id` where `purchase_order`.`company_id` = %s and `purchase_order`.`purchase_order_number` = %s and `purchase_order_item`.`line_number` = %s", (company, re.sub(r'[^\d]', '', data['P.O.^NUMBER']), data['LINE^#']))
	row = cursor.fetchone()
	if row is not None:
		notes = ""
		for note in data[2:]:
			if isset(note):
				notes = f"{notes}\n{note}"
		if isset(notes):
			cursor.execute("update `purchase_order_item` set `internal_notes` = %s where `id` = %s", (notes.strip(), row['id']))

def import_transaction(data, is_invoiced=False):
	try:
		order_date = parse(data['ORDER^DATE'])
	except:
		order_date = date.today()
	transaction = {
		'active': 1,
		'company_id': company,
		'created_at': order_date.isoformat(),
		'customer_attention': data['BILL TO ATTENTION'],
		'customer_client_name': data['CLIENT'],
		'customer_id': get_id('customer', 'legacy_customer_number', data['CUST #']),
		'customer_order_number': data['CUSTOMER^ORDER^NUMBER'],
		'fob': data['FOB'],
		'freight_amount': data['FREIGHT'],
		'freight_terms': data['FREIGHT TERMS'],
		'guaranteed_by': data['GUARANTEED BY'],
		'legacy_transaction_number': data['ORDER #'],
		'line_id': line,
		'notification_generated_at': now,
		'order_terms_id': get_code_id(13, data['TRMS^CDE']),
		'packages': data['# OF^PCK'],
		'packing_charge': data['PACK^CHG'],
		'print_notification': 0,
		'rep1_id': get_id('rep', 'rep_code', data['SALES^REP 1']),
		'rep_purchase_order_number': data['REP ORDER^NUMBER'],
		'return_code_id': get_code_id(26, data['RETURN^CODE']),
		'ship_to_attention': data['SHIP ATTENTION'],
		'ship_to_city': data['SHIP TO CITY'],
		'ship_to_country_id': get_country_id(data['SHIP-TO^COUNTRY CD']),
		'ship_to_name': data['SHIP TO NAME'],
		'ship_to_postal_code': data['SHIP TO^ZIP CODE'],
		'ship_to_street': data['SHIP TO ADDRESS 1'],
		'ship_to_street2': data['SHIP TO ADDRESS 2'],
		'sidemark': data['SIDE MARK'],
		'specifier_id': get_id('customer', 'legacy_customer_number', data['SPECIFIER^NUMBER']),
		'state_tax_percent': data['TAX^PCT'] / 100,
		'state_taxable_basis': data['TAXABLE^BASIS'],
		'transaction_type_id': 1,
		'updated_at': now,
		'user_id': user,
		'warehouse_id': warehouse_def_id,
		'weight': data['WGHT'],
		'mill_cut_yardage': '1' if data['CUT YARDAGE^ORDER'] == 'C' else '0',
	}

	if isempty(transaction['rep1_id']):
		transaction['rep1_id'] = unknown_rep['id']

	transaction['state_tax_amount'] = data['TAX^AMOUNT']
	
	if transaction['mill_cut_yardage'] == '1':
		transaction['cut_yardage_reserve'] = now
		transaction['vendor_id'] = get_id('vendor', 'legacy_vendor_code', data['CUT YARDAGE^VENDOR #'])

		if data['FAX^RPT^FLG'] in ['T','S']:
			transaction['cut_yardage_shippable'] = now

		if data['MILL^STK^FLG'] in ['Y', 'N']:
			transaction['mill_cut_yardage_received_status'] = '1'

		if data['CUTTING^ARRIVED^FLAG'] == 'Y':
			transaction['cut_yardage_cfa_arrived'] = '1'

	match = pattern_transaction_number.match(data['ORDER #'])
	if match:
		transaction['transaction_number'] = match.group(1)
		transaction['transaction_number_suffix'] = match.group(2)
		if data['RES^FLG'] == 'Y':
			transaction['transaction_type_id'] = 3
	else:
		if pattern_credit_memo.match(data['ORDER #']):
			transaction['transaction_type_id'] = 7
			transaction['invoice_count'] = -1
		elif pattern_debit_memo.match(data['ORDER #']):
			transaction['transaction_type_id'] = 8
		elif pattern_quote.match(data['ORDER #']):
			transaction['transaction_type_id'] = 2
			transaction['backordered'] = 0

		transaction['transaction_number'] = next_sequential_number('transaction_number')
		transaction['transaction_number_suffix'] = 0

	if isset(transaction['ship_to_country_id']):
		transaction['ship_to_state_id'] = get_state_id(transaction['ship_to_country_id'], data['SHIP^ST'])

	if data['SAMPLE^FLAG'] == 'Y':
		transaction['sample_order'] = 1
		transaction['transaction_type_id'] = 5

	# if data['BACK^ORDER^FLAG'] == 'B/O':
	# 	transaction['backordered'] = 1
		
	if is_invoiced:
		transaction['backordered'] = 0
	elif transaction['mill_cut_yardage'] == '1' and data['MILL^STK^FLG'] != 'N':
		transaction['backordered'] = 0

	if 'backordered' not in transaction:
		transaction['backordered'] = 1

	if data['DISC^PCT'] > 0:
		transaction['discount_percent'] = data['DISC^PCT'] / 100

	if data['D^F'] == 'L':
		transaction['discount_method'] = 'L'
	else:
		transaction['discount_method'] = 'B'

	if isset(data['RESERVE^CANCEL^DATE']):
		try:
			transaction['reserve_cancel_date'] = parse(data['RESERVE^CANCEL^DATE']).isoformat()
		except:
			pass

	if isset(data['BACK ORDER^REL DATE']):
		try:
			transaction['backorder_release_date'] = parse(data['BACK ORDER^REL DATE']).isoformat()
		except:
			pass

	transaction['carrier_id'] = get_column('customer', 'default_carrier_id', 'id', transaction['customer_id'])
	transaction['shipping_service_id'] = get_column('customer', 'default_shipping_service_id', 'id', transaction['customer_id'])

	if is_invoiced:
		transaction['status'] = 'S'
		transaction['invoice_number'] = re.sub(r'[^\d]', '', data['INVOICE^NUMBER'])
		try:
			transaction['date_shipped'] = parse(data['SHIP^DATE']).isoformat()
		except:
			print(f"Unable to import date shipped for order #{data['ORDER #']}")

		try:
			date_invoiced = parse(data['INVOICE^DATE'])
		except:
			try:
				date_invoiced = parse(data['SHIP^DATE'])
				print(f"Unable to import invoice date; using ship date ({data['ORDER #']}/{data['INVOICE^DATE']})")
			except:
				print(f"Unable to import invoice due to missing invoice date and ship date ({data['ORDER #']})")
				return

		transaction['print_pick_ticket'] = 0
		transaction['print_invoice'] = 0
		try:
			transaction['pick_ticket_generated_at'] = parse(data['PICK^TICK^DATE']).isoformat()
		except:
			transaction['pick_ticket_generated_at'] = now

		transaction['date_invoiced'] = date_invoiced.isoformat()
		transaction['date_payment_completed'] = date_invoiced.isoformat()
		transaction['invoice_generated_at'] = date_invoiced.isoformat()
		transaction['accounting_period'] = date_invoiced.strftime('%Y-%m-01')
		transaction['fiscal_year'] = date_invoiced.strftime('%Y')
		transaction['material_amount_invoiced'] = data['MATERIAL^AMOUNT']
		if transaction['discount_method'] == 'B' and 'discount_percent' in transaction:
			transaction['list_material_amount_invoiced'] = data['MATERIAL^AMOUNT'] / (1 - transaction['discount_percent'])
		else:
			transaction['list_material_amount_invoiced'] = data['MATERIAL^AMOUNT']
		transaction['discount_amount_invoiced'] = data['DISCOUNT^AMOUNT']
		transaction['service_amount_invoiced'] = data['MISCEL-^LANEOUS']
		transaction['invoice_amount'] = data['NET']
		transaction['invoice_cost'] = data['COST^OF^ORDER']
		transaction['company_invoice_cost'] = data['COST^OF^ORDER']
		transaction['commission_amount1'] = data['COMM^PAID^AMT']

		if isset(data['COMM^PAID^DATE']):
			try:
				transaction['commission_paid_date'] = parse(data['COMM^PAID^DATE']).isoformat()
			except:
				print(f"Unable to set commission paid date ({data['ORDER #']}/{data['COMM^PAID^DATE']})")

	else:
		transaction['status'] = 'O'

	transaction_id = insert_object('transaction', transaction)

	if data['DEPOSIT'] > 0:
		insert_object('transaction_payment', {
			'transaction_id': transaction_id,
			'payment_code_id': code_unknown_payment_id,
			'payment_date': transaction['created_at'],
			'amount': data['DEPOSIT'],
			'amount_applied': data['DEPOSIT'],
			'created_at': now,
			'updated_at': now
		})

	if isset(data['HLD^FLG1']):
		add_transaction_hold(transaction_id, data['HLD^FLG1'])

	if isset(data['HLD^FLG2']):
		add_transaction_hold(transaction_id, data['HLD^FLG2'])

	if data['CFA^FLG'] == 'Y':
		add_transaction_hold(transaction_id, 'CFA')

	if data['PRO^FLG'] == 'Y':
		add_transaction_hold(transaction_id, 'PRO')

def import_transaction_item(data, is_invoiced=False):
	transaction_id = get_id('transaction', 'legacy_transaction_number', data['ORDER #'])
	item_id = get_id('item', 'item_number', data['ITEM NUMBER'])

	if transaction_id is None:
		print(f"Skipping item for missing transaction #{data['ORDER #']}")
		return
	elif item_id is None:
		print(f"Skipping transaction item for missing item #{data['ITEM NUMBER']}")
		return

	if data['PRICE'] < 0:
		data['PRICE'] = abs(data['PRICE'])
		if data['QTY^ORDERED'] > 0:
			data['QTY^ORDERED'] = -1 * data['QTY^ORDERED']
		else:
			data['QTY^ORDERED'] = abs(data['QTY^ORDERED'])

	record = {
		'comments': data['TAG COMMENT'],
		'commission_percent1': data['COMM^PCT REP 1'] / 100,
		'commission_percent2': data['COMM^PCT REP 2'] / 100,
		'company_cost_of_pieces': data['UNIT^COST'],
		'cost_of_pieces': data['UNIT^COST'],
		'created_at': now,
		'customer_quantity_ordered': data['QTY^ORDERED'],
		'item_id': item_id,
		'list_price': data['PRICE'],
		'original_customer_quantity_ordered': data['QTY^ORDERED'],
		'price': data['PRICE'],
		'quantity_ordered': data['QTY^ORDERED'],
		'transaction_id': transaction_id,
		'unit_conversion_factor': 1,
		'updated_at': now,
	}

	if isset(data['MILL SHIP^DATE']):
		record['cut_yardage_mill_ship_date'] = parse(data['MILL SHIP^DATE']).isoformat()

	if isset(data['OUR SHIP^DATE']):
		record['cut_yardage_ship_date'] = parse(data['OUR SHIP^DATE']).isoformat()

	try:
		record['line_number'] = int(data['LIN^NUM'])
	except:
		print(f"Unable to import line number for {data['ORDER #']}/{data['ITEM NUMBER']}")
		record['line_number'] = 0

	record['customer_unit_id'] = get_selling_unit_id(record['item_id'])

	if is_invoiced:
		record['quantity_shipped'] = data['QTY^SHPD']
		record['customer_quantity_shipped'] = data['QTY^SHPD']
		record['replacement_unit_cost'] = max(0, data['QTY^RELEASED'])
	else:
		if isset(data['P.O.NUMBER']):
			cursor.execute("select `purchase_order_item`.`id` from `purchase_order_item` inner join `purchase_order` on `purchase_order_item`.`purchase_order_id` = `purchase_order`.`id` where `purchase_order`.`company_id` = %s and `purchase_order`.`purchase_order_number` = %s and `purchase_order_item`.`line_number` = %s", (company, re.sub(r'[^\d]', '', data['P.O.NUMBER']), data['PO^LINE']))
			row = cursor.fetchone()
			if row is not None:
				record['allocated_purchase_order_item_id'] = row['id']

	insert_object('transaction_item', record)

def import_transaction_allocation(data, log=True):
	cursor.execute("select `transaction_item`.`id`, `transaction_item`.`item_id`, `transaction`.`mill_cut_yardage` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`legacy_transaction_number` = %s and `transaction_item`.`line_number` = %s", (company, data['ORDER #'], data['LIN^NUM']))
	detail_record = cursor.fetchone()
	if detail_record is None:
		print(f"Skipping transaction allocation for missing transaction item {data['ORDER #']}/{data['LIN^NUM']}")
		return

	if isempty(data['LOT^NUMBER']) or isempty(data['PCE^NUMBER']):
		print(f"Skipping transaction allocation for missing lot/piece ({data['LOT^NUMBER']}/{data['PCE^NUMBER']})")
		return

	record = {
		'active': 0,
		'customer_quantity': data['QTY^SHPD^FROM^PCE'],
		'quantity': data['QTY^SHPD^FROM^PCE'],
		'transaction_item_id': detail_record['id'],
	}

	if detail_record['mill_cut_yardage'] == 1:
		record['cut_yardage_lot'] = data['LOT^NUMBER']
		record['cut_yardage_piece'] = data['PCE^NUMBER']
	else:
		cursor.execute("select `id` from `inventory` where `item_id` = %s and `lot` = %s and `piece` = %s", (detail_record['item_id'], data['LOT^NUMBER'], data['PCE^NUMBER']))
		inventory_record = cursor.fetchone()
		if inventory_record is None:
			if log:
				print(f"Skipping transaction allocation for missing piece {data['ORDER #']}/{detail_record['item_id']}/{data['LOT^NUMBER']}/{data['PCE^NUMBER']}")
			return
		record['inventory_id'] = inventory_record['id']

	try:
		insert_object('transaction_allocated_piece', record)
	except pymysql.err.IntegrityError:
		print(f"Skipping duplicate transaction allocation ({data['ORDER #']}/{data['LIN^NUM']})")

def import_transaction_service(data):
	transaction_id = get_id('transaction', 'legacy_transaction_number', data['ORDER #'])
	service_id = get_id('service', 'item_number', data['ITEM NUMBER'])

	if transaction_id is None:
		print(f"Skipping service for missing transaction #{data['ORDER #']}")
		return
	elif service_id is None:
		print(f"Skipping service for missing item #{data['ITEM NUMBER']}")
		return

	if data['PRICE'] < 0:
		data['PRICE'] = abs(data['PRICE'])
		if data['QTY^ORDERED'] > 0:
			data['QTY^ORDERED'] = -1 * data['QTY^ORDERED']
		else:
			data['QTY^ORDERED'] = abs(data['QTY^ORDERED'])


	record = {
		'comments': data['TAG COMMENT'],
		'created_at': now,
		'customer_quantity_ordered': data['QTY^ORDERED'],
		'customer_unit_id': get_column('service', 'unit_id', 'item_number', data['ITEM NUMBER']),
		'price': data['PRICE'],
		'quantity_ordered': data['QTY^ORDERED'],
		'service_id': service_id,
		'transaction_id': transaction_id,
		'unit_conversion_factor': 1,
		'updated_at': now,
	}

	try:
		record['line_number'] = int(data['LIN^NUM'])
	except:
		print(f"Unable to import line number for {data['ORDER #']}/{data['ITEM NUMBER']}")
		record['line_number'] = 0

	insert_object('transaction_service', record)

def import_transaction_message(data):
	transaction_id = get_id('transaction', 'legacy_transaction_number', data['ORDER #'])
	message = data['TAG COMMENT']
	if transaction_id is None:
		print(f"Unable to import message for missing transaction #{data['ORDER #']}")
		return

	if isempty(message):
		print(f"Unable to import empty message for transaction #{data['ORDER #']}")
		return

	if data['ITEM NUMBER'] == 'Z':
		cursor.execute("update `transaction` set `notification_message` = concat(ifnull(`notification_message`, ''), %s, '\n') where `id` = %s", (message, transaction_id))
		cursor.execute("update `transaction` set `pick_ticket_message` = concat(ifnull(`pick_ticket_message`, ''), %s, '\n') where `id` = %s", (message, transaction_id))
		cursor.execute("update `transaction` set `invoice_message` = concat(ifnull(`invoice_message`, ''), %s, '\n') where `id` = %s", (message, transaction_id))
	elif data['ITEM NUMBER'] == 'ZN':
		cursor.execute("update `transaction` set `notification_message` = concat(ifnull(`notification_message`, ''), %s, '\n') where `id` = %s", (message, transaction_id))
	elif data['ITEM NUMBER'] == 'ZP' or data['ITEM NUMBER'] == 'ZM':
		cursor.execute("update `transaction` set `pick_ticket_message` = concat(ifnull(`pick_ticket_message`, ''), %s, '\n') where `id` = %s", (message, transaction_id))
	elif data['ITEM NUMBER'] == 'ZI':
		cursor.execute("update `transaction` set `invoice_message` = concat(ifnull(`invoice_message`, ''), %s, '\n') where `id` = %s", (message, transaction_id))

def import_transaction_note(data):
	transaction_id = get_id('transaction', 'legacy_transaction_number', data[0])

	if transaction_id is None:
		print(f"Unable to import notes for missing transaction #{data[0]}")
		return

	note = {
		'company_id': company,
		'content': '',
		'created_at': now,
		'noteable_id': transaction_id,
		'noteable_type': 'Transaction',
		'updated_at': now,
		'user_id': user,
	}

	for column in data[1:]:
		if isset(column) and len(column.strip()) > 0:
			note['content'] = note['content'] + column.strip() + '\n'

	insert_object('note', note)

def import_accounts_receivable(data):
	customer = get_object('customer', 'customer_number', str(data['CUST^NUMB']).rjust(6, '0'))
	if customer is None:
		print(f"Cannot import A/R data for missing customer #{data['CUST^NUMB']}")
		return

	amount = data['TRANSACTION^AMOUNT']

	payment_date = parse(str(data['REF DATE']))

	record = {
		'accounting_period': payment_date.strftime('%Y-%m-01'),
		'amount': 0,
		'check_cc_number': 'Unknown',
		'company_id': company,
		'created_at': now,
		'currency_id': customer['currency_id'],
		'customer_id': customer['id'],
		'exchange_rate': 1,
		'line_id': line,
		'payment_code_id': code_unknown_payment_id,
		'payment_date': payment_date.isoformat(),
		'updated_at': now,
	}

	invoice_match = pattern_ar_invoice.match(data['REFERENCE^NUMBER'])
	payment_match = pattern_ar_payment.match(data['REFERENCE^NUMBER'])

	transaction = None

	if isset(data['ORDER^NUMBER']):
		transaction = get_object('transaction', 'legacy_transaction_number', data['ORDER^NUMBER'])

	if invoice_match is not None:
		if amount > 0 or pattern_credit_memo.match(data['REFERENCE^NUMBER']) is not None:
			cursor.execute("update `transaction` set `date_payment_completed` = null where `id` = %s", transaction['id'])
			return

		record['payment_number'] = next_sequential_number('payment_number')
		payment_id = insert_object('payment', record)

		payment_detail = {
			'amount_applied': abs(amount),
			'created_at': now,
			'payment_id': payment_id,
			'transaction_id': transaction['id'],
			'updated_at': now,
		}

		insert_object('payment_detail', payment_detail)

	elif payment_match is not None:
		if amount > 0:
			print("Offset payment")

		record['payment_number'] = payment_match.group(1)
		insert_object('payment', record)
		payment_id = get_id('payment', 'payment_number', payment_match.group(1))
		cursor.execute("update `payment` set `amount` = `amount` + %s where `id` = %s", (abs(amount), payment_id))

		payment_detail = {
			'amount_applied': abs(amount),
			'created_at': now,
			'payment_id': payment_id,
			'updated_at': now,
		}

		if transaction is not None and transaction['date_payment_completed'] is None:
			payment_detail['transaction_id'] = transaction['id']

		insert_object('payment_detail', payment_detail)
	else:
		print("Skipping useless A/R record")

start_time = time()

print("Deleting existing records...")
if config['import'].getboolean('accounts_receivable'):
	cursor.execute("delete from `payment` where `company_id` = %s", company)
	cursor.execute("update `transaction` set `date_payment_completed` = `date_invoiced` where `company_id` = %s and status = 'S'", company)

if config['import'].getboolean('shipped_transaction_notes'):
	cursor.execute("delete from `note` where `company_id` = %s and `noteable_type` = 'Transaction'", company)

if config['import'].getboolean('shipped_transaction_allocations'):
	cursor.execute("delete `transaction_allocated_piece` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` inner join `transaction_allocated_piece` on `transaction_item`.`id` = `transaction_allocated_piece`.`transaction_item_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'S'", company)
if config['import'].getboolean('shipped_transaction_detail'):
	cursor.execute("delete `transaction_item` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'S'", company)
	cursor.execute("delete `transaction_service` from `transaction` inner join `transaction_service` on `transaction`.`id` = `transaction_service`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'S'", company)
if config['import'].getboolean('shipped_transactions'):
	cursor.execute("delete `transaction_credit_memo` from `transaction` inner join `transaction_credit_memo` on `transaction`.`id` = `transaction_credit_memo`.`invoice_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'S'", company)
	cursor.execute("delete from `transaction` where `company_id` = %s and `status` = 'S'", company)

if config['import'].getboolean('open_transaction_allocations'):
	cursor.execute("delete `transaction_allocated_piece` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` inner join `transaction_allocated_piece` on `transaction_item`.`id` = `transaction_allocated_piece`.`transaction_item_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'O'", company)
if config['import'].getboolean('open_transaction_detail'):
	cursor.execute("delete `transaction_item` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'O'", company)
	cursor.execute("delete `transaction_service` from `transaction` inner join `transaction_service` on `transaction`.`id` = `transaction_service`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'O'", company)
if config['import'].getboolean('open_transactions'):
	cursor.execute("delete from `transaction` where `company_id` = %s and `status` = 'O'", company)

cursor.execute("delete `transaction_allocated_piece` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` inner join `transaction_allocated_piece` on `transaction_item`.`id` = `transaction_allocated_piece`.`transaction_item_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'C'", company)
cursor.execute("delete `transaction_item` from `transaction` inner join `transaction_item` on `transaction`.`id` = `transaction_item`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'C'", company)
cursor.execute("delete `transaction_service` from `transaction` inner join `transaction_service` on `transaction`.`id` = `transaction_service`.`transaction_id` where `transaction`.`company_id` = %s and `transaction`.`status` = 'C'", company)
cursor.execute("delete from `transaction` where `company_id` = %s and `status` = 'C'", company)

if config['import'].getboolean('purchase_order_notes'):
	cursor.execute("update `purchase_order_item` inner join `purchase_order` on `purchase_order_item`.`purchase_order_id` = `purchase_order`.`id` set `purchase_order_item`.`internal_notes` = null where `purchase_order`.`company_id` = %s", company)

if config['import'].getboolean('purchase_order_detail'):
	cursor.execute("delete `purchase_order_item` from `purchase_order` inner join `purchase_order_item` on `purchase_order`.`id` = `purchase_order_item`.`purchase_order_id` where `purchase_order`.`company_id` = %s", company)

if config['import'].getboolean('purchase_orders'):
	cursor.execute("delete from `purchase_order` where `company_id` = %s", company)

if config['import'].getboolean('inventory'):
	cursor.execute("delete from `inventory` where `company_id` = %s", company)

if config['import'].getboolean('services'):
	cursor.execute("delete from `service` where `company_id` = %s", company)

if config['import'].getboolean('items'):
	cursor.execute("delete from `item_inquiry` where `company_id` = %s", company)
	cursor.execute("delete from `item` where `company_id` = %s", company)
	cursor.execute("delete from `item_price` where `company_id` = %s and `priceable_type` = 'Item'", company)

if config['import'].getboolean('styles'):
	cursor.execute("delete `style`, `style_cost` from `style` left join `style_cost` on `style`.`id` = `style_cost`.`style_id` where `style`.`company_id` = %s", company)
	cursor.execute("delete from `item_price` where `company_id` = %s and `priceable_type` = 'Style'", company)
	cursor.execute("delete from `style_cost` where `style_id` not in (select `id` from `style`)")

if config['import'].getboolean('vendor_employees'):
	cursor.execute("delete `employee`, `email` from `vendor` inner join `employee` on `vendor`.`id` = `employee`.`employer_id` and `employee`.`employer_type` = 'Vendor' left join `email` on `employee`.`id` = `email`.`emailable_id` and `email`.`emailable_type` = 'Employee' where `vendor`.`company_id` = %s", company)

if config['import'].getboolean('vendors'):
	cursor.execute("delete `address` from `address` inner join `vendor` on `address`.`addressable_id` = `vendor`.`id` and `address`.`addressable_type` = 'Vendor' where `vendor`.`company_id` = %s", (company))
	cursor.execute("delete from `address` where `addressable_type` = 'Vendor' and `addressable_id` not in (select `id` from `vendor`)")
	cursor.execute("delete `phone` from `phone` inner join `vendor` on `phone`.`phoneable_id` = `vendor`.`id` and `phone`.`phoneable_type` = 'Vendor' where `vendor`.`company_id` = %s", (company))
	cursor.execute("delete from `phone` where `phoneable_type` = 'Vendor' and `phoneable_id` not in (select `id` from `vendor`)")
	cursor.execute("delete from `vendor` where `company_id` = %s", company)

if config['import'].getboolean('customer_resale'):
	cursor.execute("delete `resale_certificate` from `resale_certificate` inner join `customer` on `resale_certificate`.`customer_id` = `customer`.`id` inner join `primary_address` on `resale_certificate`.`customer_id` = `primary_address`.`addressable_id` and `primary_address`.`addressable_type` = 'Customer' where `customer`.`company_id` = %s and (`resale_certificate`.`state_id` != `primary_address`.`state_id` or `resale_number` = 'On File')", company)

if config['import'].getboolean('customer_ship_tos'):
	cursor.execute("delete `ship_to`, `address` from `customer` inner join `ship_to` on `customer`.`id` = `ship_to`.`customer_id` left join `address` on `ship_to`.`id` = `address`.`addressable_id` and `address`.`addressable_type` = 'ShipTo' where `customer`.`company_id` = %s", company)
	cursor.execute("delete from `address` where `addressable_type` = 'ShipTo' and `addressable_id` not in (select `id` from `ship_to`)")

if config['import'].getboolean('customer_employees'):
	cursor.execute("delete `employee`, `email`, `phone` from `customer` inner join `employee` on `customer`.`id` = `employee`.`employer_id` and `employee`.`employer_type` = 'Customer' left join `email` on `employee`.`id` = `email`.`emailable_id` and `email`.`emailable_type` = 'Employee' left join `phone` on `employee`.`id` = `phone`.`phoneable_id` and `phone`.`phoneable_type` = 'Employee' where `customer`.`company_id` = %s", company)

if config['import'].getboolean('customer_ar_notes'):
	cursor.execute("delete from `note` where `company_id` = %s and `noteable_type` = 'Customer' and `type` = 'A/R'", company)

if config['import'].getboolean('customer_notes'):
	cursor.execute("delete from `note` where `company_id` = %s and `noteable_type` = 'Customer' and `type` is null", company)

if config['import'].getboolean('customers'):
	cursor.execute("delete `address` from `address` inner join `customer` on `address`.`addressable_id` = `customer`.`id` and `address`.`addressable_type` = 'Customer' where `customer`.`company_id` = %s", (company))
	cursor.execute("delete ignore from `address` where `addressable_type` = 'Customer' and `addressable_id` not in (select `id` from `customer`)")
	cursor.execute("delete `phone` from `phone` inner join `customer` on `phone`.`phoneable_id` = `customer`.`id` and `phone`.`phoneable_type` = 'Customer' where `customer`.`company_id` = %s", (company))
	cursor.execute("delete from `phone` where `phoneable_type` = 'Customer' and `phoneable_id` not in (select `id` from `customer`)")
	cursor.execute("delete from `customer` where `company_id` = %s", company)

if config['import'].getboolean('rep_employees'):
	cursor.execute("delete `employee`, `email`, `phone` from `rep` inner join `employee` on `rep`.`id` = `employee`.`employer_id` and `employee`.`employer_type` = 'Rep' left join `email` on `employee`.`id` = `email`.`emailable_id` and `email`.`emailable_type` = 'Employee' left join `phone` on `employee`.`id` = `phone`.`phoneable_id` and `phone`.`phoneable_type` = 'Employee' where `rep`.`company_id` = %s", company)

if config['import'].getboolean('reps'):
	cursor.execute("delete from `rep_discount_commission` where `company_id` = %s", company)
	cursor.execute("delete `address` from `address` inner join `rep` on `address`.`addressable_id` = `rep`.`id` and `address`.`addressable_type` = 'Rep' where `rep`.`company_id` = %s", (company))
	cursor.execute("delete from `address` where `addressable_type` = 'Rep' and `addressable_id` not in (select `id` from `rep`)")
	cursor.execute("delete `phone` from `phone` inner join `rep` on `phone`.`phoneable_id` = `rep`.`id` and `phone`.`phoneable_type` = 'Rep' where `rep`.`company_id` = %s", (company))
	cursor.execute("delete from `phone` where `phoneable_type` = 'Rep' and `phoneable_id` not in (select `id` from `rep`)")
	cursor.execute("delete from `rep` where `company_id` = %s", company)

if config['import'].getboolean('warehouses'):
	cursor.execute("delete from `warehouse` where `company_id` = %s", company)

if config['import'].getboolean('codes'):
	cursor.execute("delete from `code` where `editable` = 1 and `company_id` = %s", company)
	cursor.execute("delete from `carrier` where `company_id` = %s", company)
	cursor.execute("delete from `shipping_service` where `company_id` = %s", company)

db.commit()

currency_usd_id = get_id('currency', 'code', 'USD', False)
country_us_id = get_id('country', 'code', 'US', False)

carrier_ups_id = get_id('carrier', 'name', 'UPS', False)
carrier_fedex_id = get_id('carrier', 'name', 'FedEx', False)
carrier_other_id = get_id('carrier', 'name', 'Other', False)

shipping_ground_id = get_id('shipping_service', 'name', 'Ground', False)
shipping_next_id = get_id('shipping_service', 'name', 'Next Day', False)
shipping_second_id = get_id('shipping_service', 'name', '2nd Day', False)
shipping_third_id = get_id('shipping_service', 'name', '3rd Day', False)
shipping_other_id = get_id('shipping_service', 'name', 'Other', False)

cutoff_date = datetime(1970, 1, 1)

code_unknown_terms_id = get_unknown_code(13)
code_unknown_unit_id = get_unknown_code(15)
code_unknown_product_category_id = get_unknown_code(1)
code_unknown_payment_id = get_unknown_code(8)
code_yard_id = get_code_id(15, 'Yard')
code_meter_id = get_code_id(15, 'Meter')
code_each_id = get_code_id(15, 'Each')
code_discontinued_id = get_code_id(7, 'Discontinued')
code_standard_package_id = create_code_id('STD', 20, 'Standard')

pattern_us_zip = re.compile(r'^[\d]{5}')
pattern_deleted_rep = re.compile(r'^z', re.IGNORECASE)
pattern_deleted_customer = re.compile(r'\*{2,}\s+?(closed|filming complete)', re.IGNORECASE)
pattern_discontinue_code = re.compile(r"(([A-Z]+)-)?([\d]{2}/[\d]{2}(/[\d]{2,4})?)")
pattern_credit_memo = re.compile(r"^CM [\d]{6}$")
pattern_debit_memo = re.compile(r"^DM [\d]{6}$")
pattern_quote = re.compile(r"^QU [\d]{6}$")
pattern_transaction_number = re.compile(r"^([\d]{6})-([\d]{2})$")
pattern_ar_invoice = re.compile(r"^(DI|[DC]M)\s([\d]{6})")
pattern_ar_payment = re.compile(r"^CP\s([\d]{6})")
pattern_valid_unit = re.compile(r"^[a-z]", re.IGNORECASE)

if config['import'].getboolean('codes'):
	print("Importing codes...")
	codes = get_iterator(data_directory + 'MISCCODE.TXT')
	for index,code in codes:
		import_code(code)
	db.commit()
	cursor.execute("update `code` set `abbreviation` = 'UNK' where `company_id` = %s and `name` = 'Unknown'", company)
	cursor.execute("update `code` set `at_proforma` = 1 where `company_id` = %s and `type_id` = 13 and `name` like %s", (company, '%forma%'))
	cursor.execute("update `code` set `at_proforma` = 0 where `company_id` = %s and `type_id` = 13 and `at_proforma` is null", company)
	cursor.execute("update `code` set `pkg_length` = 36, `pkg_width` = 6, `pkg_height` = 6 where `company_id` = %s and `type_id` = 20", company)
	cursor.execute("update `line` set `default_package_code_id` = %s where `company_id` = %s", (code_standard_package_id, company))
carrier_def_id = get_column('company_default', 'customer_carrier_id', 'company_id', company)
shipping_service_def_id = get_column('company_default', 'customer_shipping_service_id', 'company_id', company)

if config['import'].getboolean('warehouses'):
	print("Importing warehouses...")
	warehouses = get_iterator(data_directory + 'MISCCODE.TXT')
	for index,warehouse in warehouses:
		if warehouse['TYPE'] == 'W':
			import_warehouse(warehouse)
	cursor.execute("update `line` set `default_warehouse_id` = (select `id` from `warehouse` where `company_id` = %s and `code` = %s limit 1) where `company_id` = %s", (company, default_warehouse_code, company))
	db.commit()
warehouse_def_id = get_column('line', 'default_warehouse_id', 'id', line)

if config['import'].getboolean('vendors'):
	print("Importing vendors...")
	vendors = get_iterator(data_directory + 'VENDOR.TXT')
	for index,vendor in vendors:
		import_vendor(vendor)
	for vendor_id,check_to_code in vendor_check_to.items():
		check_vendor_id = get_id('vendor', 'legacy_vendor_code', check_to_code)
		if check_vendor_id is not None:
			cursor.execute(f"update `vendor` set `send_check_vendor_id` = %s where `id` = %s", (check_vendor_id, vendor_id))
	cursor.execute(f"insert into `vendor` (`company_id`, `legacy_vendor_code`, `name`, `country_id`, `currency_id`, `delivery_days`, `date_established`, `terms_id`) values ({company}, %s, %s, {country_us_id}, {currency_usd_id}, 0, curdate(), {code_unknown_terms_id})", ("UNK", "Unknown"))
	unknown_vendor = get_object('vendor', 'id', cursor.lastrowid)
	db.commit()
else:
	unknown_vendor = get_object('vendor', 'legacy_vendor_code', 'UNK')

if config['import'].getboolean('vendor_employees'):
	print("Importing vendor employees...")
	employees = get_iterator(data_directory + 'VENDEML.TXT')
	for index,employee in employees:
		import_vendor_employee(employee)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('reps'):
	print("Importing reps...")
	reps = get_iterator(data_directory + 'SALESREP.TXT')
	for index,rep in reps:
		import_rep(rep)
	cursor.execute(f"insert into `rep` (`company_id`, `rep_code`, `name`, `country_id`, `currency_id`, `start_date`) values ({company}, %s, %s, {country_us_id}, {currency_usd_id}, curdate())", ("UNK", "Unknown"))
	unknown_rep = get_object('rep', 'id', cursor.lastrowid)
	db.commit()
else:
	unknown_rep = get_object('rep', 'rep_code', 'UNK')

if config['import'].getboolean('rep_employees'):
	print("Importing rep employees...")
	employees = get_iterator(data_directory + 'REPEML.TXT')
	for index,employee in employees:
		import_rep_employee(employee)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('customers'):
	print("Importing customers...")
	customers = get_iterator(data_directory + 'CUSTOMER.TXT')
	for index,customer in customers:
		import_customer(customer)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('customer_notes'):
	print("Importing customer notes...")
	notes = get_iterator(data_directory + 'CUSNOTES.TXT')
	for index,note in notes:
		import_customer_note(note)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('customer_ar_notes'):
	print("Importing customer A/R notes...")
	notes = get_iterator(data_directory + 'ARNOTES.TXT')
	for index,note in notes:
		import_customer_note(note, 'A/R')
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('customer_employees'):
	print("Importing customer employees...")
	employees = get_iterator(data_directory + 'CUSTEML.TXT')
	for index,employee in employees:
		import_customer_employee(employee)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('customer_ship_tos'):
	print("Importing customer ship to addresses...")
	addresses = get_iterator(data_directory + 'CUSTSHTO.TXT')
	for index,address in addresses:
		import_customer_ship_to_address(address)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('customer_resale'):
	print("Importing customer out-of-state resale certificates...")
	records = get_iterator(data_directory + 'RESALNO.TXT')
	for index,record in records:
		import_customer_resale(record)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('styles'):
	print("Importing styles...")
	styles = get_iterator(data_directory + 'PATTERN.TXT', 0, 0)
	for index,style in styles:
		import_style(style)
		if index % 500 == 0:
			db.commit()
	harmonized = get_iterator(data_directory + 'HARMONIZ.TXT')
	for index,style in harmonized:
		import_harmonized(style)
		if index % 500 == 0:
			db.commit()
	cursor.execute("update `code` inner join `style` on `code`.`id` = `style`.`selling_unit_id` set `code`.`um_selling_unit` = 1 where `code`.`company_id` = %s", (company))
	cursor.execute("update `code` inner join `style` on `code`.`id` = `style`.`mill_unit_id` set `code`.`um_mill_unit` = 1 where `code`.`company_id` = %s", (company))
	cursor.execute("insert into `style` (`company_id`, `line_id`, `name`, `product_category_code_id`, `default_warehouse_id`, `selling_unit_id`, `mill_unit_id`, `vendor_id`, `standard_quantity`, `legacy_style_number`) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (company, line, 'Unknown', code_unknown_product_category_id, warehouse_def_id, code_unknown_unit_id, code_unknown_unit_id, unknown_vendor['id'], 1, 'UNK'))
	unknown_style = get_object('style', 'id', cursor.lastrowid)
else:
	unknown_style = get_object('style', 'legacy_style_number', 'UNK')

if config['import'].getboolean('items'):
	print("Importing items...")
	items = get_iterator(data_directory + 'COLOR.TXT')
	for index,item in items:
		import_item(item)
		if index % 500 == 0:
			db.commit()
	cursor.execute("update `style` set `deleted_at` = now() where `id` not in (select `style_id` from `item` where `discontinue_code_id` is null and `company_id` = %s group by `style_id`) and `company_id` = %s", (company, company))

if config['import'].getboolean('services'):
	print("Importing services...")
	services = get_iterator(data_directory + 'SERVICES.TXT')
	for index,service in services:
		import_service(service)
	db.commit()

if config['import'].getboolean('inventory'):
	print("Importing inventory...")
	records = get_iterator(data_directory + 'LOTPCE.TXT')
	for index,record in records:
		import_inventory(record)
		if index % 500 == 0:
			db.commit()
	cursor.execute("update `inventory` inner join `item` on `inventory`.`item_id` = `item`.`id` inner join `style` on `item`.`style_id` = `style`.`id` set `inventory`.`vendor_id` = `style`.`vendor_id` where `inventory`.`company_id` = %s", company)
	cursor.execute("update `inventory` inner join `vendor` on `inventory`.`vendor_id` = `vendor`.`id` inner join `warehouse` on `inventory`.`warehouse_id` = `warehouse`.`id` inner join `company` on `inventory`.`company_id` = `company`.`id` inner join `currency` as `vendor_currency` on `vendor`.`currency_id` = `vendor_currency`.`id` inner join `currency` as `warehouse_currency` on `warehouse`.`currency_id` = `warehouse_currency`.`id` inner join `currency` as `company_currency` on `company`.`currency_id` = `company_currency`.`id` set `exchange_rate_entry` = `vendor_currency`.`exchange_rate` / `warehouse_currency`.`exchange_rate`, `company_exchange_rate_entry` = `company_currency`.`exchange_rate` / `warehouse_currency`.`exchange_rate`, `inventory`.`currency_id` = `warehouse`.`currency_id` where `inventory`.`company_id` = %s", company)
	cursor.execute("update `inventory` set `exchange_rate_receipt` = `exchange_rate_entry`, `company_exchange_rate_receipt` = `company_exchange_rate_entry` where `inventory`.`company_id` = %s", company)
	db.commit()

if config['import'].getboolean('purchase_orders'):
	print("Importing purchase orders...")
	purchase_orders = get_iterator(data_directory + 'MILLPOH.TXT')
	for index,purchase_order in purchase_orders:
		import_purchase_order(purchase_order)
		if index % 500 == 0:
			db.commit()
	db.commit()
if config['import'].getboolean('purchase_order_detail'):
	print("Importing purchase order items...")
	rows = get_iterator(data_directory + 'MILLPOD.TXT')
	for index,row in rows:
		import_purchase_order_item(row)
		if index % 500 == 0:
			db.commit()
	db.commit()
if config['import'].getboolean('purchase_order_notes'):
	print("Importing purchase order notes...")
	rows = get_iterator(data_directory + 'POINOTES.TXT')
	for index,row in rows:
		import_purchase_order_note(row)
		if index % 500 == 0:
			db.commit()
	db.commit()

# Transactions
if config['import'].getboolean('open_transactions'):
	print("Importing open transactions...")
	transactions = get_iterator(data_directory + 'ORDOPNH.TXT')
	for index,transaction in transactions:
		import_transaction(transaction)
		if index % 500 == 0:
			db.commit()
	db.commit()
if config['import'].getboolean('shipped_transactions'):
	print("Importing shipped transactions...")
	transactions = get_iterator(data_directory + 'ORDSHPH.TXT')
	for index,transaction in transactions:
		import_transaction(transaction, True)
		if index % 500 == 0:
			db.commit()
	db.commit()

# Transaction Services
if config['import'].getboolean('open_transaction_detail'):
	print("Importing open transaction services...")
	rows = get_iterator(data_directory + 'OPENOSRV.TXT')
	for index,row in rows:
		import_transaction_service(row)
		if index % 2000 == 0:
			db.commit()
	db.commit()
if config['import'].getboolean('shipped_transaction_detail'):
	print("Importing shipped transaction services...")
	rows = get_iterator(data_directory + 'SHIPOSRV.TXT')
	for index,row in rows:
		import_transaction_service(row)
		if index % 2000 == 0:
			db.commit()
	db.commit()

# Transaction Items
if config['import'].getboolean('open_transaction_detail'):
	print("Importing open transaction items...")
	rows = get_iterator(data_directory + 'ORDOPND.TXT')
	for index,row in rows:
		import_transaction_item(row)
		if index % 2000 == 0:
			db.commit()
	db.commit()
if config['import'].getboolean('shipped_transaction_detail'):
	print("Importing shipped transaction items...")
	rows = get_iterator(data_directory + 'ORDSHPD.TXT')
	for index,row in rows:
		import_transaction_item(row, True)
		if index % 2000 == 0:
			db.commit()
	db.commit()
	
# Transaction Allocations
if config['import'].getboolean('open_transaction_allocations'):
	print("Importing open transaction allocations...")
	rows = get_iterator(data_directory + 'PCECMTD.TXT')
	for index,row in rows:
		import_transaction_allocation(row)
		if index % 500 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('shipped_transaction_allocations'):
	print("Importing shipped transaction allocations...")
	rows = get_iterator(data_directory + 'PCESHIP.TXT')
	for index,row in rows:
		import_transaction_allocation(row, False)
		if index % 2000 == 0:
			db.commit()
	db.commit()

# Transaction Messages
if config['import'].getboolean('open_transaction_messages'):
	print("Importing open transaction messages...")
	rows = get_iterator(data_directory + 'OPENORDZ.TXT')
	for index,row in rows:
		import_transaction_message(row)
		if index % 500 == 0:
			db.commit()
	db.commit()
if config['import'].getboolean('shipped_transaction_messages'):
	print("Importing shipped transaction messages...")
	rows = get_iterator(data_directory + 'SHIPORDZ.TXT')
	for index,row in rows:
		import_transaction_message(row)
		if index % 2000 == 0:
			db.commit()
	db.commit()

# Transaction Notes
if config['import'].getboolean('shipped_transaction_notes'):
	print("Importing shipped transaction notes...")
	rows = get_iterator(data_directory + 'ORDNOTES.TXT', None)
	for index,row in rows:
		import_transaction_note(row)
		if index % 2000 == 0:
			db.commit()
	db.commit()

if config['import'].getboolean('open_transactions') or config['import'].getboolean('shipped_transactions'):
	print("Finalizing imported transactions...")
	cursor.execute("update `transaction` inner join `customer` on `transaction`.`customer_id` = `customer`.`id` set `transaction`.`sale_type_id` = `customer`.`type_id` where `transaction`.`company_id` = %s and `transaction`.`sale_type_id` is null", company)
	cursor.execute("update `transaction` inner join `customer` on `transaction`.`customer_id` = `customer`.`id` set `transaction`.`currency_id` = `customer`.`currency_id` where `transaction`.`company_id` = %s", company)
	cursor.execute("update `transaction` inner join `currency` on `transaction`.`currency_id` = `currency`.`id` set `transaction`.`exchange_rate_entered` = 1 / `currency`.`exchange_rate`, `transaction`.`exchange_rate_invoiced` = 1 / `currency`.`exchange_rate` where `transaction`.`company_id` = %s and `transaction`.`status` = 'S'", company)
	cursor.execute("update `transaction` inner join `currency` on `transaction`.`currency_id` = `currency`.`id` set `transaction`.`exchange_rate_entered` = 1 / `currency`.`exchange_rate` where `transaction`.`company_id` = %s and `transaction`.`status` = 'O'", company)
	cursor.execute("update `transaction` inner join `transaction_hold` on `transaction`.`id` = `transaction_hold`.`transaction_id` set `transaction_hold`.`released_by_user_id` = %s, `transaction_hold`.`released_at` = now() where `transaction`.`company_id` = %s and `transaction`.`status` = 'S'", (user, company))
	cursor.execute(f"update `transaction` set `client_auth_key` = replace(uuid() collate {config['mysql']['collation']}, '-', '') where `company_id` = %s and `client_auth_key` is null", company)
	cursor.execute("update `transaction` left join (select `transaction_id`, max(`line_number`) as `max_line_number` from `transaction_item` group by `transaction_id`) as `items` on `transaction`.`id` = `items`.`transaction_id` left join (select `transaction_id`, max(`line_number`) as `max_line_number` from `transaction_service` group by `transaction_id`) as `services` on `transaction`.`id` = `services`.`transaction_id` set `transaction`.`next_line_number` = ifnull(greatest(`items`.`max_line_number`, `services`.`max_line_number`), 0) + 1 where `transaction`.`company_id` = %s", company)
	cursor.execute("update `code` set `h_hold_type` = 'G' where `h_hold_type` is null and `company_id` = %s and `type_id` = 10", (company))

if config['import'].getboolean('accounts_receivable'):
	print("Importing accounts receivable...")
	rows = get_iterator(data_directory + 'AROPEN.TXT')
	for index,row in rows:
		import_accounts_receivable(row)
		if index % 500 == 0:
			db.commit()
	db.commit()

cursor.close()
db.commit()
db.close()

elapsed_time = timedelta(seconds=int(time() - start_time))
print(f"Finished in {str(elapsed_time)}")