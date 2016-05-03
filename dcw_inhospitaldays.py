from mylib import *
import time
import datetime
from bson.objectid import ObjectId

state=['NO','HAD','6MONTHAGO','LAST6MONTH','LAST1MONTH','LAST1WEEK','YES']


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
	DIC=sql.get_dict_list('dic_list.txt')
	coll = mgo.get_collection()

	hospital_id_name = {}
	data = sql.get_data_by_query('SELECT id,name FROM da_hospital')
	for d in data:
		hospital_id_name[d[0]]=d[1].strip()
	section_id_name = {}
	data = sql.get_data_by_query('SELECT value,label FROM sys_dict where type="base_dept"')
	for d in data:
		section_id_name[d[0]]=d[1].strip()
	
	#pprint.pprint(hospital_id_name)
	#pprint.pprint(section_id_name)
	Hos=[]
	section=[]
	with codecs.open('hospital_section_list.txt',encoding='utf-8') as f:
		f.readline()
		for each in f:
			lines=each.strip().split()
			Hos.append(lines[0])
			section.append(lines[1])
	approve_Map ={}
	print('get_approve_Map')
	for i in range(len(Hos)):
		#print(Hos[i]+' '+section[i])
		sql.get_approve_Map(approve_Map ,query='select A.ref_id,substr(A.submit_time,1,7),substr(A.approve_time,1,7),A.member_id,A.state \
		from dcw_base as A join da_member as B ON A.member_id = B.id \
		where  B.real_hospital="'+Hos[i]+'" and B.real_dept="'+section[i]+'" and A.state=5 and A.repeat_state!=4 ')
		print(len(approve_Map))
	count=0
	querys=''
	for case in coll.find({},{"caseNo":1,"patient":1,"medical":1,"surgical":1}):
		#print('"'+str(case['_id'])+'",')
		admission_date=None
		admission_date_str=''
		disCharge_date=None
		disCharge_date_str=''
		Doctor_name=''
		hospital_id=''
		section_id=''
		hospital_name=''
		section_name=''
		days=0
		if str(case['_id']) not in approve_Map.keys() or approve_Map[str(case['_id'])]['state'] in ["0","1"]:
			continue

		if 'basic' in case['patient'] and 'admissionDate' in case['patient']['basic']:
			try:
				admission_date=str_to_datetime(case['patient']['basic']['admissionDate'])
				admission_date_str=admission_date.strftime("%Y-%m-%d")
			except:
				pass
			
		if 'basic' in case['patient'] and 'dischargeDate' in case['patient']['basic']:
			try:
				disCharge_date=str_to_datetime(case['patient']['basic']['dischargeDate'])
				disCharge_date_str=disCharge_date.strftime("%Y-%m-%d")
			except:
				pass

		try:
			days=(disCharge_date-admission_date).days

		except Exception as e:
			#print(e)
			days=-1

		if 'majorDoctor' in case['patient']:
			try:
				Doctor_name=case['patient']['majorDoctor']['name']
			except:
				pass
			try:
				hospital_id=case['patient']['majorDoctor']['hospital']
				hospital_name=hospital_id_name[hospital_id]
			except:
				pass
			try:
				section_id=case['patient']['majorDoctor']['section']
				section_name=section_id_name[section_id]
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
		disease_name=''

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
		no_disease=False
		try:
			dis = case['patient']['disease']['details']
		except:
			no_disease=True
		if no_disease:
			disease_name='未知'
		else:
			doublecancer=0
			dindex=-1

			for i in range(len(dis)):
				if 'name' in dis[i].keys() and  dis[i]['name']!="":
					doublecancer=doublecancer+1
					dindex=i
			if doublecancer>=2:
				disease_name='多源发'
			elif doublecancer==0:
				disease_name='未知'
			elif doublecancer==1:
				if 'name' in dis[dindex].keys():
					if dis[dindex]['name'] in DIC['cancer'].keys():
						disease_name=DIC['cancer'][dis[dindex]['name']]
					else:
						disease_name=dis[dindex]['name']

			else:
				disease_name='未知'
		#print(submit_hospital_name+':'+submit_section_name+':'+case_hospital_name+':'+case_section_name)
		
		H_D='`submit_hospital_id`,`submit_hospital_name`,`submit_section_id`,`submit_section_name`,`case_hospital_id`,`case_hospital_name`,`case_section_id`,`case_section_name`,`disease_name`'
		H_D_data='"'+submit_hospital_id+'"'+','+'"'+submit_hospital_name+'"'+','+'"'+submit_section_id+'"'+','+'"'+submit_section_name+'"'+','+'"'+case_hospital_id+'"'+','+'"'+case_hospital_name+'"'+','+'"'+case_section_id+'"'+','+'"'+case_section_name+'"'+','+'"'+disease_name+'"'
		


		query='`dcw_id`,`major_doctor`,`admission_date`,`discharge_date`,`days`,`doctor_hospital_id`,`doctor_hospital_name`,`doctor_section_id`,`doctor_section_name`,'+H_D
		query='REPLACE INTO dcw_inhospitaldays ('+query+') VALUES('+'"'+case['caseNo']+'"'+','+'"'+Doctor_name+'"'+','+'"'+admission_date_str+'"'+','+'"'+disCharge_date_str+'"'
		query=query+','+str(days)+','+'"'+hospital_id+'"'+','+'"'+hospital_name+'"'+','+'"'+section_id+'"'+','+'"'+section_name+'",'+H_D_data+');'


		#print(query)
		
		querys=querys+query+'\n'
		'''
		try:
			sql2.cur.execute(query)
			sql2.conn.commit()
		except Exception as e:
			print(e)
			print('插入出问题')
			sql2.conn.rollback()
		'''
		
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