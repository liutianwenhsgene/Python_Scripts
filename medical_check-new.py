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

	coll = mgo.get_collection()
	side_score={}


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
		where  B.hospital="'+Hos[i]+'" and B.dept="'+section[i]+'" and A.state=5 and A.repeat_state!=4 ')
	
	count=0
	#print(coll.count({"patient.basic.hospital":Dept_list[each_D]['Hospital_id'],"patient.basic.hospitalSection":Dept_list[each_D]['Dept_id']}))
	for case in coll.find({},{"caseNo":1,"patient":1,"medical":1,"surgical":1}):
		M_list=['chrt','targetedTherapy','radiotherapy','endocrineTherapy','immunotherapy']
		M_value={'chrt':'NO','targetedTherapy':'NO','radiotherapy':'NO','endocrineTherapy':'NO','immunotherapy':'NO','surgical':'NO'}
	
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
								elif (admission_date-this_time).days<=7:
									M_value[M]= state[max(state.index('LAST1WEEK'),state.index(M_value[M]))]
								elif (admission_date-this_time).days<=30:
									M_value[M]= state[max(state.index('LAST1MONTH'),state.index(M_value[M]))]
								elif (admission_date-this_time).days<=180:
									M_value[M]= state[max(state.index('LAST6MONTH'),state.index(M_value[M]))]
								elif (admission_date-this_time).days>180:
									M_value[M]= state[max(state.index('6MONTHAGO'),state.index(M_value[M]))]
									
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
						elif (admission_date-this_time).days<=7:
							M_value['surgical']= state[max(state.index('LAST1WEEK'),state.index(M_value['surgical']))]
						elif (admission_date-this_time).days<=30:
							M_value['surgical']= state[max(state.index('LAST1MONTH'),state.index(M_value['surgical']))]
						elif (admission_date-this_time).days<=180:
							M_value['surgical']= state[max(state.index('LAST6MONTH'),state.index(M_value['surgical']))]
						elif (admission_date-this_time).days>180:
							M_value['surgical']= state[max(state.index('6MONTHAGO'),state.index(M_value['surgical']))]
							
					except:
						pass
					if M_value['surgical']=='YES':
						break
		except:
			pass

		query=''
		for each in M_value.keys():
			query=query+',`'+each+'`'

		query='INSERT INTO dcw_medical_check (`dcw_id`,`admission_date`,`admission_month`'+query+') VALUES('+'"'+case['caseNo']+'"'+','+'"'+admission_date_str+'"'+','+'"'+admission_month+'"'
		for each in M_value.keys():
			query=query+','+'"'+M_value[each]+'"'
		query = query+')'
		#print(query)
		try:
			sql2.cur.execute(query)
			sql2.conn.commit()
		except Exception as e:
			print(e)
			print('插入出问题')
			sql2.conn.rollback()
		count+=1
		if count%1000==0:
			print(count)
	print(count)

def test1():
	print('start')
	mgo=mongobase()
	coll = mgo.get_collection()
	data = coll.find_one({},{"caseNo":1,"patient":1,"medical":1,"inspection":1})
	pprint.pprint(str(data['_id'])) 

if __name__=='__main__':
	test()