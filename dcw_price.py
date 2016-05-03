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

	CN_to_ID = {}
	data = sql.get_data_by_query('SELECT id,name_cn FROM kn_drug_info')
	for d in data:
		if len(d[1])>0:
			CN_to_ID[d[1]]=d[0].strip()
	EN_to_ID = {}
	data = sql.get_data_by_query('SELECT id,name FROM kn_drug_info')
	for d in data:
		if len(d[1])>0:
			EN_to_ID[d[1]]=d[0].strip()
	alias_to_ID = {}
	data = sql.get_data_by_query('SELECT id,alias_name FROM kn_drug_info')
	for d in data:
		if len(d[1])>0:
			alias_to_ID[d[1]]=d[0].strip()

	#pprint.pprint(CN_to_ID)
	#pprint.pprint(EN_to_ID)

	ID_to_Price = {}
	ID_to_unit ={}
	data = sql2.get_data_by_query('SELECT id,average_price,unit FROM drug_price')
	for d in data:
		if len(d[0])>0:
			ID_to_Price[str(d[0])]=float(d[1])
			ID_to_unit[str(d[0])]=d[2]
	#pprint.pprint(ID_to_Price)
	#pprint.pprint(ID_to_unit)
	ID_to_Price_per_day={}
	ID_to_Price_per_mg={}
	data = sql2.get_data_by_query('SELECT id,day_price,mg_price FROM drug_price')
	for d in data:
		if len(d[0])>0:
			ID_to_Price_per_day[str(d[0])]=float(d[1])
			ID_to_Price_per_mg[str(d[0])]=float(d[2])
	
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
	for case in coll.find({"caseNo":"DCW-2015-00318"},{"caseNo":1,"patient":1,"medical":1,"surgical":1}):
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
			
		chrt_medicine_dose={}
		chrt_medicine_time={}
		chrt_INFO_STATE='NORMAL'
		chrt_estimate=0
		chrt_cost=0
		target_medicine_dose={}
		target_medicine_time={}
		target_INFO_STATE='NORMAL'
		target_estimate=0
		target_cost=0

		if 'medical' in case.keys():
			if 'chrt' in case['medical'].keys():
				for each_chrt in case['medical']['chrt']:
					if  'time' in each_chrt and admission_date!=None and str_to_datetime(each_chrt['time'])!=None and str_to_datetime(each_chrt['time'])>=admission_date:
						try:
							if 'period' in each_chrt.keys():
								period=int(each_chrt['period'])
							else:
								period=0
						except:
							period=0
						try:
							if 'cost' in each_chrt:
								chrt_cost+=float(each_chrt['cost'])
						except:
							pass
						if 'medicinePlan' in each_chrt:
							for each_medicine in each_chrt['medicinePlan']:
								try:
									if 'medicineName' in each_medicine and len(each_medicine['medicineName'])>0:
										medicine=each_medicine['medicineName']
										if medicine not in chrt_medicine_dose:
											chrt_medicine_dose[medicine]=0
											chrt_medicine_time[medicine]=period
										else:
											chrt_medicine_time[medicine]+=period
										try:
											if 'totalDose' in each_medicine.keys():
												chrt_medicine_dose[medicine]+=float(each_medicine['totalDose'])
										except:
											pass
								except:
									pass

			if 'targetedTherapy' in case['medical'].keys():

				for each_target in case['medical']['targetedTherapy']:
					
					if admission_date!=None and  'time' in each_target and str_to_datetime(each_target['time'])!=None and str_to_datetime(each_target['time'])>=admission_date:
						if 'name' in each_target and len(each_target['name'])>0:
							medicine=each_target['name']
							#print(medicine)
							if medicine not in target_medicine_dose:
								target_medicine_time[medicine]=0
								target_medicine_dose[medicine]=0
							if 'medicine' in each_target:
								if 'duration' in each_target['medicine'] and len(str(each_target['medicine']['duration']))>0:
									target_medicine_time[medicine]+=float(each_target['medicine']['duration'])
								if 'totalDose' in each_target['medicine'] and len(str(each_target['medicine']['totalDose']))>0:
									target_medicine_dose[medicine]+=float(each_target['medicine']['totalDose'])

								if 'cost' in each_target['medicine'] :
									if len(str(each_target['medicine']['cost']))>0:
										target_cost+=float(each_target['medicine']['cost'])

					
					
		for each in chrt_medicine_dose.keys():
			medicineid=''
			if each in CN_to_ID.keys():
				medicineid=CN_to_ID[each]
			elif each in EN_to_ID.keys():
				medicineid=EN_to_ID[each]
			elif each in alias_to_ID.keys():
				medicineid=alias_to_ID[each]
			elif each in ID_to_Price.keys():
				medicineid=each
			else:
				chrt_INFO_STATE='MATCHIDERROR'
				continue
			if ID_to_unit[medicineid]=='MG':
				if chrt_medicine_dose[each]<=0:
					chrt_INFO_STATE='DOSEMISSING'
					if chrt_medicine_time[each]>0:
						chrt_estimate+=ID_to_Price_per_day[medicineid]*chrt_medicine_time[each]
				else:
					chrt_estimate+=(ID_to_Price[medicineid]*chrt_medicine_dose[each])
			elif ID_to_unit[medicineid]=='DAY':
				if chrt_medicine_time[each]<=0:
					chrt_INFO_STATE='PERIODMISSING'
					if chrt_medicine_dose[each]>0:
						chrt_estimate+=ID_to_Price_per_mg[medicineid]*chrt_medicine_dose[each]
				else:
					chrt_estimate+=(ID_to_Price[medicineid]*chrt_medicine_time[each])
			elif ID_to_unit[medicineid]=='COURSE':
				if chrt_medicine_time[each]>0:
					chrt_estimate+=(ID_to_Price[medicineid]*(chrt_medicine_time[each]/21))
				elif chrt_medicine_dose[each]>0:
					chrt_estimate+=ID_to_Price_per_mg[medicineid]*chrt_medicine_dose[each]
				else:
					chrt_estimate+=ID_to_Price[medicineid]

		if len(chrt_medicine_dose)==0:
			chrt_INFO_STATE='NOMEDICINE'
		fee=0
		if chrt_cost>0:
			fee=chrt_cost
		elif chrt_estimate>0:
			fee=chrt_estimate
		else:
			fee=-1
		chrt_cost_rank=''
		if fee==-1:
			chrt_cost_rank='NOINFO'
		elif fee>0 and fee<1000:
			chrt_cost_rank='LT1000'
		elif fee>=1000 and fee<3000:
			chrt_cost_rank='F1000T3000'
		elif fee>=3000 and fee<4000:
			chrt_cost_rank='F3000T4000'
		elif fee>=4000 and fee<7000:
			chrt_cost_rank='F4000T7000'
		elif fee>=7000:
			chrt_cost_rank='GT7000'

		#pprint.pprint(chrt_medicine_time)
		#pprint.pprint(chrt_medicine_dose)

		for each in target_medicine_dose.keys():
			medicine_id=''
			if each in CN_to_ID.keys():
				medicineid=CN_to_ID[each]
			elif each in EN_to_ID.keys():
				medicineid=EN_to_ID[each]
			elif each in alias_to_ID.keys():
				medicineid=alias_to_ID[each]
			elif each in ID_to_Price.keys():
				medicineid=each
			else:
				target_INFO_STATE='MATCHIDERROR'
				continue

			if ID_to_unit[medicineid]=='MG':
				if target_medicine_dose[each]<0:
					target_INFO_STATE='DOSEMISSING'
				else:
					target_estimate+=(ID_to_Price[medicineid]*target_medicine_dose[each])
			elif ID_to_unit[medicineid]=='DAY':
				if target_medicine_time[each]<0:
					target_INFO_STATE='PERIODMISSING'
				else:
					target_estimate+=(ID_to_Price[medicineid]*target_medicine_time[each])
			elif ID_to_unit[medicineid]=='COURSE':
				target_estimate+=ID_to_Price[medicineid]
		if len(target_medicine_dose)==0:
			target_INFO_STATE='NOMEDICINE'


		fee=0
		if target_cost>0:
			fee=target_cost
		elif target_estimate>0:
			fee=target_estimate
		else:
			fee=-1
		target_cost_rank=''
		if fee<=0:
			target_cost_rank='NOINFO'
		elif fee>0 and fee<1000:
			target_cost_rank='LT1000'
		elif fee>=1000 and fee<3000:
			target_cost_rank='F1000T3000'
		elif fee>=3000 and fee<4000:
			target_cost_rank='F3000T4000'
		elif fee>=4000 and fee<7000:
			target_cost_rank='F4000T7000'
		elif fee>=7000:
			target_cost_rank='GT7000'

		#pprint.pprint(target_medicine_time)
		#pprint.pprint(target_medicine_dose)

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
		

		query='`dcw_id`,`admission_date`,`admission_month`,`chrt_medicine_num`,`chrt_cost`,`chrt_estimate`,`chrt_info_state`,`target_medicine_num`,`target_cost`,`target_estimate`,`target_info_state`,`chrt_cost_rank`,`target_cost_rank`,'+H_D
		query='REPLACE INTO dcw_price ('+query+') VALUES('+'"'+case['caseNo']+'"'+','+'"'+admission_date_str+'"'+','+'"'+admission_month+'"'
		query=query+','+str(len(chrt_medicine_dose))+','+str(chrt_cost)+','+str(chrt_estimate)+','+'"'+chrt_INFO_STATE+'"'
		query=query+','+str(len(target_medicine_dose))+','+str(target_cost)+','+str(target_estimate)+','+'"'+target_INFO_STATE+'"'+','+'"'+chrt_cost_rank+'"'+','+'"'+target_cost_rank+'",'+H_D_data+');'

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