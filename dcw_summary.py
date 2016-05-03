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
	print(sql.host)
	print('get_title')
	titlelist = get_list_by_file('base_info.txt')
	titleMap = get_map_by_file('base_info.txt')
	coll = mgo.get_collection()
	print('bulid dic')
	DIC=sql.get_dict_list('dic_list.txt')
	print('get approve')
	area_type=sql.get_dict('select id,type from sys_area')
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
		print(len(approve_Map))
	count=0
	querys=''
	pprint.pprint('get mongo')
	for case in coll.find({},{"caseNo":1,"patient":1}):

		record=Record()
		if str(case['_id']) not in approve_Map.keys():
			continue
		count+=1
		for eachcol in titlelist:
			if titleMap[eachcol]['base'] == 'null':
				continue
			truestr = get_ture_str(eachcol,case,approve_Map,DIC,titleMap)
			if eachcol.find('date')>=0 and len(truestr) > 0:
				time=None
				try:
					time = str_to_datetime(truestr)
				finally:
					try:
						truestr = time.strftime("%Y-%m-%d")
						#print(truestr)
					except:
						trustr=''

				record.set_info(eachcol,truestr)

			elif eachcol.find('date')>=0 and len(truestr) == 0:
				record.set_info(eachcol,truestr)
			elif len(truestr)>0:
				record.set_info(eachcol,truestr)
			else:
				record.set_info(eachcol,'未知')

		T1=None
		T2=None
		d=0
		try:
			T1=str_to_datetime(str(case['patient']['basic']['admissionDate']))
			str_T1=T1.strftime("%Y-%m")
			record.set_info('admission_month',str_T1)
		except:
			pass#print("日期缺失1")

		try:
			T2=str_to_datetime(str(case['patient']['disease']['symptom']['firstAdmissionDate']))
			str_T2=T2.strftime("%Y-%m")
			record.set_info('first_admission_month',str_T2)
		except:
			pass#print("日期缺失2")
		#print(T1)
		#print(T2)
		try:
			d=(T1-T2).days
			if d <0:
				d=0
		except Exception as e:
			#print(e)
			if T1==None and T2 !=None:
				d=-1
			elif T1!=None and T2==None:
				d=-2
			elif T1==None and T2==None:
				d=-3
			else:
				d=-4
			pass#print("日期缺失3")
		survival_period=''
		if d==-1:
			survival_period='AD_MISSING'
		elif d==-2:
			survival_period='FAD_MISSING'
		elif d==-3:
			survival_period='FAD_AD_MISSING'
		elif d==-4:
			survival_period='UNKNOWN'
		elif d==0:
			survival_period='FAD_EQUAL_AD'
		elif 0<d<30:
			survival_period='LT1MONTH'
		elif d<90:
			survival_period='F1T3MONTH'
		elif d<365:
			survival_period='F3T12MONTH'
		elif d<365*3:
			survival_period='F1T3YEAR'
		elif d<365*5:
			survival_period='F3T5YEAR'
		elif d<365*10:
			survival_period='F5T10YEAR'
		else :
			survival_period='GTE10YEAR'

		record.set_info('survival_period',survival_period)
		
		try:
			s_t=approve_Map[str(case['_id'])]['submit_time']
			record.set_info('submit_month',s_t)
		except:
			pass#print("提交日期缺失")

		try:
			a_t=approve_Map[str(case['_id'])]['approve_time']
			record.set_info('audit_month',a_t)
		except:
			pass#print("审核日期缺失")

		age = -1
		age_label='UNKNOWN'
		try:
			age=int(case['patient']['basic']['admissionAge'])
		except :
			try:
				age=T1.year-T2.year
			except:
				age = -1#print('年龄缺失')
		if age==-1:
			age_label="UNKNOWN"
		elif age<30:
			age_label="LT30"
		elif age>=70:
			age_label="GTE70"
		else:
			age_label='F'+str(age-age%10)+'T'+str(age-age%10+9)

		record.set_info('age_stage',age_label)
		no_disease=False
		try:
			dis = case['patient']['disease']['details']
		except:
			no_disease=True
		if no_disease:
			record.set_info('double_primary',"信息缺失")
		else:
			doublecancer=0
			dindex=-1

			for i in range(len(dis)):
				if 'name' in dis[i].keys() and  dis[i]['name']!="":
					doublecancer=doublecancer+1
					dindex=i
			if doublecancer>=2:
				record.set_info('double_primary',"是")
				record.set_info('disease_name','未知')
				record.set_info("disease_phase",'未知')
			elif doublecancer==0:
				record.set_info('double_primary',"信息缺失")
				record.set_info('disease_name','未知')
				record.set_info("disease_phase",'未知')
			elif doublecancer==1:
				record.set_info('double_primary',"否")
				if 'name' in dis[dindex].keys():
					if dis[dindex]['name'] in DIC['cancer'].keys():
						record.set_info('disease_name',DIC['cancer'][dis[dindex]['name']])
					else:
						record.set_info('disease_name',dis[dindex]['name'])
				try:
					record.set_info("disease_phase",DIC['cancer_stages'][dis[dindex]['phase'][0]])
				except:
					pass#print("疾病分期缺失")
			else:
				record.set_info('disease_name','未知')
				record.set_info("disease_phase",'未知')
		record.set_info('reside_address','其他')
		try:
			ralist=case['patient']['basic']['resideAddress']
			for each_ra in ralist:
				if area_type[each_ra]!='district':
					record.set_info('reside_address','其他')
				elif area_type[each_ra]=='district':
					record.set_info('reside_address',DIC['area'][each_ra])
					break
		except:
			pass

		querys=querys+sql.get_a_record_sql(record)+'\n'

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