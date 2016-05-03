from mylib import *
import time
import datetime
from bson.objectid import ObjectId
'''
age_stage
0:

'''
def str_to_datetime(s):
	A=None
	try:
		A=datetime.datetime.strptime(s,'%Y-%m-%d')
	except:
		try:
			A=datetime.datetime.strptime(s,'%Y-%m')
		except:
			try:
				A=datetime.datetime.strptime(s,'%Y')
			except:
				pass
	return A


def test():
	print('start')
	sql = sqlbase("databases.config","<source-mysql>")
	mgo = mongobase("databases.config","<source-mongo>")
	sql2= sqlbase("databases.config","<target-mysql>")

	coll = mgo.get_collection()
	side_score={}

	DIC=sql.get_dict_list('dic_list.txt')

	Hos=[]
	section=[]
	with codecs.open('hospital_section_list.txt',encoding='utf-8') as f:
		f.readline()
		for each in f:
			lines=each.strip().split()
			Hos.append(lines[0])
			section.append(lines[1])
	approve_Map ={}
	for i in range(len(Hos)):
		#print(Hos[i]+' '+section[i])
		sql.get_approve_Map(approve_Map ,query='select A.ref_id,substr(A.submit_time,1,7),substr(A.approve_time,1,7),A.member_id,A.state \
		from dcw_base as A join da_member as B ON A.member_id = B.id \
		where  B.real_hospital="'+Hos[i]+'" and B.real_dept="'+section[i]+'" and A.state=5 and A.repeat_state!=4 ')
	
	count=0
	querys=''
	for case in coll.find({},{"caseNo":1,"patient":1,"medical":1,"surgical":1}):
		M_list=['chrt','targeted_therapy','radio_therapy','endocrine_therapy','immuno_therapy']
		M_value={'chrt':'NO','targeted_therapy':'NO','radio_therapy':'NO','endocrine_therapy':'NO','immuno_therapy':'NO','surgical':'NO'}
	
		admission_date=None
		admission_date_str=''
		admission_month=''
		if str(case['_id']) not in approve_Map.keys() or approve_Map[str(case['_id'])]['state'] in ["0","1"]:
			continue

		if 'basic' in case['patient'] and 'admissionDate' in case['patient']['basic']:
			try:
				admission_date=str_to_datetime(case['patient']['basic']['admissionDate'])
				admission_date_str=admission_date.strftime("%Y-%m-%d")
				admission_month=admission_date.strftime("%Y-%m")
			except:
				pass
			
		if 'medical' in case.keys():
			for M in M_list:
				if M in case['medical'].keys():
					try:
						for each in case['medical'][M]:
							if len(each)>0:
								M_value[M]='HAD'
							try:
								this_time=str_to_datetime(each['time'])
								if this_time>=admission_date:
									M_value[M]='YES'
							except:
								pass
							if M_value[M]=='YES':
									break
					except:
						pass
		try:
			if 'surgical' in case.keys():

				for each in case['surgical']:
					if len(each)>0: 
						M_value['surgical']='HAD'
						#print(each['operationTime'])
					try:
						this_time=str_to_datetime(each['operationTime'])
						if this_time>=admission_date:
							M_value['surgical']='YES'
					except:
						pass
					if M_value['surgical']=='YES':
						break
		except:
			pass


		submit_hospital_id=''
		submit_hospital_name=''
		submit_section_id=''
		submit_section_name=''
		case_hospital_id=''
		case_hospital_name=''
		case_section_id=''
		case_section_name=''

		try:
			submit_hospital_id=DIC['M_H'][approve_Map[str(case['_id'])]['member_id']]
			submit_hospital_name=DIC['hos'][submit_hospital_id]
		except:
			pass
		try:
			submit_section_id=DIC['M_D'][approve_Map[str(case['_id'])]['member_id']]
			submit_section_name=DIC['dept'][submit_section_id]
		except:
			pass
		try:
			case_hospital_id=case['patient']['basic']['hospital']
			case_hospital_name=DIC['hos'][case_hospital_id]
		except:
			pass
		try:
			case_section_id=case['patient']['basic']['hospitalSection']
			case_section_name=DIC['dept'][case_section_id]
		except:
			pass

		#print(submit_hospital_name+':'+submit_section_name+':'+case_hospital_name+':'+case_section_name)
		
		H_D='`submit_hospital_id`,`submit_hospital_name`,`submit_section_id`,`submit_section_name`,`case_hospital_id`,`case_hospital_name`,`case_section_id`,`case_section_name`'
		H_D_data='"'+submit_hospital_id+'"'+','+'"'+submit_hospital_name+'"'+','+'"'+submit_section_id+'"'+','+'"'+submit_section_name+'"'+','+'"'+case_hospital_id+'"'+','+'"'+case_hospital_name+'"'+','+'"'+case_section_id+'"'+','+'"'+case_section_name+'"'
		query=''
		for each in M_value.keys():
			query=query+',`'+each+'`'

		query='REPLACE INTO dcw_medical_check (`dcw_id`,`admission_date`,`admission_month`'+query+','+H_D+') VALUES('+'"'+case['caseNo']+'"'+','+'"'+admission_date_str+'"'+','+'"'+admission_month+'"'
		for each in M_value.keys():
			query=query+','+'"'+M_value[each]+'"'
		query = query+','+H_D_data+');'

		querys=querys+query+'\n'

		count+=1
		if count%1000==0:
			print(count)
			try:
				sql2.cur.execute(querys)
				sql2.conn.commit()
			except Exception as e:
				print(e)
				print('插入出问题')
				sql2.conn.rollback()
			querys=''
	print(count)
	try:
		sql2.cur.execute(querys)
		sql2.conn.commit()
	except Exception as e:
		print(e)
		print('插入出问题')
		sql2.conn.rollback()
	

def test1():
	print('start')
	mgo=mongobase()
	coll = mgo.get_collection()
	data = coll.find_one({},{"caseNo":1,"patient":1,"medical":1,"inspection":1})
	pprint.pprint(str(data['_id'])) 

if __name__=='__main__':
	test()